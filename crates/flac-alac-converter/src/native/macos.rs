use crate::native::{Controls, Encoding, Verification, full_scale_sample};
use crate::types::{AudioSpec, Warning};
use crate::wav::WavSource;
use anyhow::{Context, Result, bail, ensure};
use sha2::{Digest, Sha256};
use std::ffi::{c_int, c_void};
use std::fs::{self, File, OpenOptions, Permissions};
use std::io::{Read, Seek, SeekFrom};
use std::mem::size_of;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Path, PathBuf};
use std::ptr;
use std::sync::atomic::Ordering;

const READ_CONTINUE: c_int = 0;
const READ_END_OF_STREAM: c_int = 1;
const READ_ABORT: c_int = 2;
const WRITE_CONTINUE: c_int = 0;
const WRITE_ABORT: c_int = 1;
const PCM_BLOCK_LIMIT: usize = 65_535;
const VERIFICATION_READ_FRAMES: u32 = 4_096;

const AUDIO_FORMAT_LPCM: u32 = fourcc(*b"lpcm");
const AUDIO_FORMAT_ALAC: u32 = fourcc(*b"alac");
const AUDIO_FILE_M4A: u32 = fourcc(*b"m4af");
const EXT_FILE_DATA_FORMAT: u32 = fourcc(*b"ffmt");
const EXT_CLIENT_DATA_FORMAT: u32 = fourcc(*b"cfmt");
const EXT_FILE_LENGTH_FRAMES: u32 = fourcc(*b"#frm");
const AUDIO_FORMAT_FLAG_SIGNED_INTEGER: u32 = 1 << 2;
const AUDIO_FORMAT_FLAG_PACKED: u32 = 1 << 3;
const AUDIO_FORMAT_FLAG_ALIGNED_HIGH: u32 = 1 << 4;
const ALAC_DEPTH_16: u32 = 1;
const ALAC_DEPTH_20: u32 = 2;
const ALAC_DEPTH_24: u32 = 3;
const ALAC_DEPTH_32: u32 = 4;

type ExtAudioFileRef = *mut c_void;
type CfUrlRef = *const c_void;

#[repr(C)]
#[derive(Clone, Copy, Default)]
struct AudioStreamBasicDescription {
    sample_rate: f64,
    format_id: u32,
    format_flags: u32,
    bytes_per_packet: u32,
    frames_per_packet: u32,
    bytes_per_frame: u32,
    channels_per_frame: u32,
    bits_per_channel: u32,
    reserved: u32,
}

#[repr(C)]
struct AudioBuffer {
    number_channels: u32,
    data_byte_size: u32,
    data: *mut c_void,
}

#[repr(C)]
struct AudioBufferListOne {
    number_buffers: u32,
    first: AudioBuffer,
}

#[repr(C)]
struct FlacFrameHeader {
    blocksize: u32,
    sample_rate: u32,
    channels: u32,
    channel_assignment: c_int,
    bits_per_sample: u32,
    number_type: c_int,
    number: FlacFrameNumber,
    crc: u8,
}

#[repr(C)]
union FlacFrameNumber {
    frame_number: u32,
    sample_number: u64,
}

type FlacReadCallback =
    Option<unsafe extern "C" fn(*const c_void, *mut u8, *mut usize, *mut c_void) -> c_int>;
type FlacSeekCallback = Option<unsafe extern "C" fn(*const c_void, u64, *mut c_void) -> c_int>;
type FlacTellCallback = Option<unsafe extern "C" fn(*const c_void, *mut u64, *mut c_void) -> c_int>;
type FlacLengthCallback =
    Option<unsafe extern "C" fn(*const c_void, *mut u64, *mut c_void) -> c_int>;
type FlacEofCallback = Option<unsafe extern "C" fn(*const c_void, *mut c_void) -> c_int>;
type FlacWriteCallback = Option<
    unsafe extern "C" fn(*const c_void, *const c_void, *const *const i32, *mut c_void) -> c_int,
>;
type FlacMetadataCallback = Option<unsafe extern "C" fn(*const c_void, *const c_void, *mut c_void)>;
type FlacErrorCallback = Option<unsafe extern "C" fn(*const c_void, c_int, *mut c_void)>;

unsafe extern "C" {
    fn FLAC__stream_decoder_new() -> *mut c_void;
    fn FLAC__stream_decoder_delete(decoder: *mut c_void);
    fn FLAC__stream_decoder_set_md5_checking(decoder: *mut c_void, value: c_int) -> c_int;
    fn FLAC__stream_decoder_init_stream(
        decoder: *mut c_void,
        read_callback: FlacReadCallback,
        seek_callback: FlacSeekCallback,
        tell_callback: FlacTellCallback,
        length_callback: FlacLengthCallback,
        eof_callback: FlacEofCallback,
        write_callback: FlacWriteCallback,
        metadata_callback: FlacMetadataCallback,
        error_callback: FlacErrorCallback,
        client_data: *mut c_void,
    ) -> c_int;
    fn FLAC__stream_decoder_process_until_end_of_stream(decoder: *mut c_void) -> c_int;
    fn FLAC__stream_decoder_finish(decoder: *mut c_void) -> c_int;

    fn CFURLCreateFromFileSystemRepresentation(
        allocator: *const c_void,
        buffer: *const u8,
        buffer_size: isize,
        is_directory: u8,
    ) -> CfUrlRef;
    fn CFRelease(value: *const c_void);

    fn ExtAudioFileCreateWithURL(
        url: CfUrlRef,
        file_type: u32,
        file_format: *const AudioStreamBasicDescription,
        channel_layout: *const c_void,
        flags: u32,
        output_file: *mut ExtAudioFileRef,
    ) -> i32;
    fn ExtAudioFileOpenURL(url: CfUrlRef, output_file: *mut ExtAudioFileRef) -> i32;
    fn ExtAudioFileDispose(file: ExtAudioFileRef) -> i32;
    fn ExtAudioFileGetProperty(
        file: ExtAudioFileRef,
        property_id: u32,
        property_size: *mut u32,
        property_data: *mut c_void,
    ) -> i32;
    fn ExtAudioFileSetProperty(
        file: ExtAudioFileRef,
        property_id: u32,
        property_size: u32,
        property_data: *const c_void,
    ) -> i32;
    fn ExtAudioFileWrite(
        file: ExtAudioFileRef,
        frames: u32,
        data: *const AudioBufferListOne,
    ) -> i32;
    fn ExtAudioFileRead(
        file: ExtAudioFileRef,
        frames: *mut u32,
        data: *mut AudioBufferListOne,
    ) -> i32;
}

pub(super) fn encode(
    input: &Path,
    output: &Path,
    spec: &AudioSpec,
    output_bits: u32,
    warnings: Vec<Warning>,
    controls: &mut Controls<'_>,
) -> Result<Encoding> {
    check_cancelled(controls)?;
    let input_file = open_input(input)?;
    let input_snapshot = FileSnapshot::read(&input_file)?;
    (controls.progress)(0).context("reporting initial conversion progress")?;

    let output_path = prepare_new_output(output)?;
    let output_url = file_url(&output_path)?;
    let mut writer = create_alac_writer(output_url.0, spec, output_bits)?;
    let output_file = secure_output_handle(&output_path)?;

    let decoded = decode_to_writer(
        input_file,
        input_snapshot,
        spec,
        output_bits,
        &mut writer,
        controls,
    )?;
    writer.finish()?;
    let output_snapshot = FileSnapshot::read(&output_file)?;
    ensure!(
        output_snapshot.is_regular,
        "AudioToolbox output is not a regular file"
    );

    let output_spec = inspect_output_header(&output_path, output_bits)?;
    ensure!(
        output_spec.sample_rate == spec.sample_rate
            && output_spec.channels == spec.channels
            && output_spec.bits_per_sample == output_bits
            && output_spec.frames == spec.frames,
        "AudioToolbox wrote ALAC properties that do not match the requested stream"
    );

    Ok(Encoding {
        output: output_spec,
        input_pcm_sha256: hex_digest(decoded.pcm_sha256),
        warnings,
    })
}

pub(super) fn encode_wav(
    input: &Path,
    output: &Path,
    source: &WavSource,
    output_bits: u32,
    warnings: Vec<Warning>,
    controls: &mut Controls<'_>,
) -> Result<Encoding> {
    check_cancelled(controls)?;
    let spec = &source.metadata.audio;
    let input_file = open_wav_input(input)?;
    let input_snapshot = FileSnapshot::read(&input_file)?;
    (controls.progress)(0).context("reporting initial conversion progress")?;

    let output_path = prepare_new_output(output)?;
    let output_url = file_url(&output_path)?;
    let mut writer = create_alac_writer(output_url.0, spec, output_bits)?;
    let output_file = secure_output_handle(&output_path)?;

    let decoded = encode_wav_to_writer(
        input_file,
        input_snapshot,
        source,
        output_bits,
        &mut writer,
        controls,
    )?;
    writer.finish()?;
    let output_snapshot = FileSnapshot::read(&output_file)?;
    ensure!(
        output_snapshot.is_regular,
        "AudioToolbox output is not a regular file"
    );

    let output_spec = inspect_output_header(&output_path, output_bits)?;
    ensure!(
        output_spec.sample_rate == spec.sample_rate
            && output_spec.channels == spec.channels
            && output_spec.bits_per_sample == output_bits
            && output_spec.frames == spec.frames,
        "AudioToolbox wrote ALAC properties that do not match the requested WAVE stream"
    );
    Ok(Encoding {
        output: output_spec,
        input_pcm_sha256: hex_digest(decoded.pcm_sha256),
        warnings,
    })
}

fn encode_wav_to_writer(
    mut input: File,
    input_snapshot: FileSnapshot,
    source: &WavSource,
    output_bits: u32,
    writer: &mut ExtAudioFileGuard,
    controls: &mut Controls<'_>,
) -> Result<DecodeResult> {
    let spec = &source.metadata.audio;
    let channels = usize::try_from(spec.channels).context("WAVE channel count does not fit")?;
    let sample_bytes = usize::try_from(source.container_bits / 8)
        .context("WAVE sample width does not fit this platform")?;
    let block_align = usize::try_from(source.block_align)
        .context("WAVE block alignment does not fit this platform")?;
    ensure!(
        block_align == sample_bytes.saturating_mul(channels),
        "WAVE PCM layout changed after parsing"
    );
    let max_block_frames = 4096usize;
    let mut hasher = Sha256::new();
    let mut decoded_frames = 0_u64;

    for chunk in &source.data_chunks {
        check_cancelled(controls)?;
        ensure!(
            chunk.bytes % u64::from(source.block_align) == 0,
            "WAVE data chunk contains an incomplete sample frame"
        );
        input
            .seek(SeekFrom::Start(chunk.offset))
            .context("seek to WAVE PCM data")?;
        let mut remaining_frames = chunk.bytes / u64::from(source.block_align);
        while remaining_frames > 0 {
            check_cancelled(controls)?;
            let frames = usize::try_from(remaining_frames.min(max_block_frames as u64))
                .context("WAVE block frame count does not fit this platform")?;
            let sample_count = frames
                .checked_mul(channels)
                .context("WAVE sample count overflow")?;
            let input_bytes = frames
                .checked_mul(block_align)
                .context("WAVE input block byte size overflow")?;
            let mut raw = Vec::new();
            raw.try_reserve_exact(input_bytes)
                .context("allocate bounded WAVE input block")?;
            raw.resize(input_bytes, 0);
            input
                .read_exact(&mut raw)
                .context("read complete WAVE PCM block")?;

            let mut pcm = Vec::<i32>::new();
            pcm.try_reserve_exact(sample_count)
                .context("allocate bounded canonical WAVE PCM block")?;
            for sample_bytes in raw.chunks_exact(sample_bytes) {
                let sample = decode_wav_sample(sample_bytes, spec.bits_per_sample)?;
                let full_scale = full_scale_sample(sample, spec.bits_per_sample)
                    .context("WAVE sample is outside its declared precision")?;
                pcm.push(full_scale);
            }
            ensure!(pcm.len() == sample_count, "WAVE sample block is incomplete");
            update_pcm_hash(&mut hasher, &pcm);
            let packed = packed_pcm_bytes(&pcm, output_bits)?;
            let byte_size = u32::try_from(packed.len())
                .context("packed WAVE PCM block exceeds AudioToolbox limits")?;
            let frame_count =
                u32::try_from(frames).context("WAVE frame block exceeds AudioToolbox limits")?;
            let buffers = AudioBufferListOne {
                number_buffers: 1,
                first: AudioBuffer {
                    number_channels: spec.channels,
                    data_byte_size: byte_size,
                    data: packed.as_ptr() as *mut c_void,
                },
            };
            let status = unsafe { ExtAudioFileWrite(writer.0, frame_count, &buffers) };
            check_os_status(status, "writing WAVE PCM to AudioToolbox ALAC")?;
            decoded_frames = decoded_frames
                .checked_add(u64::from(frame_count))
                .context("WAVE decoded frame count overflow")?;
            ensure!(
                decoded_frames <= spec.frames,
                "WAVE PCM data exceeds its declared frame count"
            );
            remaining_frames -= u64::from(frame_count);
            (controls.progress)(decoded_frames).context("reporting conversion progress")?;
        }
    }
    ensure!(
        decoded_frames == spec.frames,
        "WAVE yielded {decoded_frames} frames, expected {}",
        spec.frames
    );
    ensure!(
        FileSnapshot::read(&input)? == input_snapshot,
        "WAVE input identity or length changed while it was being encoded"
    );
    Ok(DecodeResult {
        pcm_sha256: hasher.finalize().into(),
    })
}

fn decode_wav_sample(bytes: &[u8], bits: u32) -> Result<i32> {
    let sample = match bits {
        8 => {
            ensure!(bytes.len() == 1, "WAVE 8-bit sample width changed");
            i32::from(bytes[0]) - 128
        }
        16 => {
            ensure!(bytes.len() == 2, "WAVE 16-bit sample width changed");
            i32::from(i16::from_le_bytes(bytes.try_into().unwrap()))
        }
        24 => {
            ensure!(bytes.len() == 3, "WAVE 24-bit sample width changed");
            let unsigned =
                u32::from(bytes[0]) | (u32::from(bytes[1]) << 8) | (u32::from(bytes[2]) << 16);
            ((unsigned << 8) as i32) >> 8
        }
        32 => {
            ensure!(bytes.len() == 4, "WAVE 32-bit sample width changed");
            i32::from_le_bytes(bytes.try_into().unwrap())
        }
        _ => bail!("unsupported WAVE PCM precision {bits}"),
    };
    Ok(sample)
}

pub(super) fn verify(
    output: &Path,
    encoding: &Encoding,
    controls: &mut Controls<'_>,
) -> Result<Verification> {
    check_cancelled(controls)?;
    let output_path = canonical_output_path(output)?;
    let file_guard = open_existing_output(&output_path)?;
    let before = FileSnapshot::read(&file_guard)?;
    let url = file_url(&output_path)?;
    let mut reader = open_alac_reader(url.0)?;

    let file_format = get_file_format(reader.0)?;
    let output_spec = spec_from_file_format(file_format, get_file_frames(reader.0)?)?;
    ensure!(
        output_spec == encoding.output,
        "ALAC file format properties changed between encoding and verification"
    );
    ensure!(
        output_spec.bits_per_sample == encoding.output.bits_per_sample,
        "ALAC source precision does not match the encoded specification"
    );
    set_pcm32_client_format(reader.0, &output_spec)?;

    let channel_count = usize::try_from(output_spec.channels)
        .context("ALAC channel count does not fit this platform")?;
    let capacity = (VERIFICATION_READ_FRAMES as usize)
        .checked_mul(channel_count)
        .context("ALAC verification buffer size overflow")?;
    let mut pcm = Vec::<i32>::new();
    pcm.try_reserve_exact(capacity)
        .context("allocating bounded ALAC verification buffer")?;
    pcm.resize(capacity, 0);
    let byte_size = u32::try_from(capacity * size_of::<i32>())
        .context("ALAC verification buffer byte size overflow")?;
    let mut buffers = AudioBufferListOne {
        number_buffers: 1,
        first: AudioBuffer {
            number_channels: output_spec.channels,
            data_byte_size: byte_size,
            data: pcm.as_mut_ptr().cast(),
        },
    };
    let mut hasher = Sha256::new();
    let mut decoded_frames = 0_u64;

    loop {
        check_cancelled(controls)?;
        buffers.first.number_channels = output_spec.channels;
        buffers.first.data_byte_size = byte_size;
        buffers.first.data = pcm.as_mut_ptr().cast();
        let mut frames = VERIFICATION_READ_FRAMES;
        let status = unsafe { ExtAudioFileRead(reader.0, &mut frames, &mut buffers) };
        check_os_status(status, "decoding ALAC verification frames")?;
        if frames == 0 {
            break;
        }
        let frame_samples = usize::try_from(frames)
            .context("ALAC decoded frame count does not fit this platform")?
            .checked_mul(channel_count)
            .context("ALAC decoded sample count overflow")?;
        ensure!(
            frame_samples <= pcm.len(),
            "AudioToolbox exceeded the verification buffer"
        );
        let next_frames = decoded_frames
            .checked_add(u64::from(frames))
            .context("ALAC decoded frame count overflow")?;
        ensure!(
            next_frames <= output_spec.frames,
            "ALAC decoded more frames than its declared file length"
        );
        for sample in &pcm[..frame_samples] {
            let _ = sample;
        }
        update_pcm_hash(&mut hasher, &pcm[..frame_samples]);
        decoded_frames = next_frames;
        (controls.progress)(decoded_frames).context("reporting ALAC verification progress")?;
    }

    reader.finish()?;
    let after = FileSnapshot::read(&file_guard)?;
    ensure!(
        before == after,
        "ALAC output identity or length changed while verification was reading it"
    );
    ensure!(
        decoded_frames == output_spec.frames,
        "ALAC decoder produced {decoded_frames} frames, expected {}",
        output_spec.frames
    );
    ensure!(
        decoded_frames == encoding.output.frames,
        "ALAC decoded frame count does not match the encoded source"
    );
    let digest = hasher.finalize();
    let output_pcm_sha256 = hex_digest(digest.into());
    ensure!(
        output_pcm_sha256 == encoding.input_pcm_sha256,
        "ALAC decoded PCM SHA-256 differs from the FLAC input PCM"
    );

    Ok(Verification {
        output: output_spec,
        output_pcm_sha256,
        frames: decoded_frames,
        bit_exact: true,
    })
}

struct DecodeResult {
    pcm_sha256: [u8; 32],
}

struct DecodeContext<'controls, 'refs> {
    input: File,
    input_snapshot: FileSnapshot,
    expected: &'controls AudioSpec,
    output_bits: u32,
    controls: &'controls mut Controls<'refs>,
    writer: ExtAudioFileRef,
    hasher: Sha256,
    frames: u64,
    callback_error: Option<String>,
    decoder_error: Option<c_int>,
}

fn decode_to_writer(
    input: File,
    input_snapshot: FileSnapshot,
    expected: &AudioSpec,
    output_bits: u32,
    writer: &mut ExtAudioFileGuard,
    controls: &mut Controls<'_>,
) -> Result<DecodeResult> {
    let mut context = DecodeContext {
        input,
        input_snapshot,
        expected,
        output_bits,
        controls,
        writer: writer.0,
        hasher: Sha256::new(),
        frames: 0,
        callback_error: None,
        decoder_error: None,
    };
    let decoder = unsafe { FLAC__stream_decoder_new() };
    ensure!(
        !decoder.is_null(),
        "libFLAC could not allocate a stream decoder"
    );
    let mut decoder = FlacDecoder {
        pointer: decoder,
        initialized: false,
    };
    ensure!(
        unsafe { FLAC__stream_decoder_set_md5_checking(decoder.pointer, 1) } != 0,
        "libFLAC refused to enable STREAMINFO MD5 verification"
    );

    let init_status = unsafe {
        FLAC__stream_decoder_init_stream(
            decoder.pointer,
            Some(flac_read_callback),
            None,
            None,
            None,
            None,
            Some(flac_write_callback),
            None,
            Some(flac_error_callback),
            (&mut context as *mut DecodeContext<'_, '_>).cast(),
        )
    };
    ensure!(
        init_status == 0,
        "libFLAC stream decoder initialization failed with status {init_status}"
    );
    decoder.initialized = true;

    let processed =
        unsafe { FLAC__stream_decoder_process_until_end_of_stream(decoder.pointer) } != 0;
    let md5_valid = decoder.finish();
    if let Some(error) = context.callback_error.take() {
        bail!("FLAC decode callback failed: {error}");
    }
    if let Some(status) = context.decoder_error {
        bail!("libFLAC reported a frame or stream error (status {status})");
    }
    ensure!(
        processed,
        "libFLAC could not decode the complete FLAC stream"
    );
    ensure!(md5_valid, "FLAC STREAMINFO PCM MD5 verification failed");
    ensure!(context.frames > 0, "FLAC stream decoded no audio frames");
    ensure!(
        context.frames == expected.frames,
        "FLAC decoded {} frames, expected {}",
        context.frames,
        expected.frames
    );
    ensure!(
        FileSnapshot::read(&context.input)? == context.input_snapshot,
        "FLAC input identity or length changed while it was being decoded"
    );
    Ok(DecodeResult {
        pcm_sha256: context.hasher.finalize().into(),
    })
}

unsafe extern "C" fn flac_read_callback(
    _decoder: *const c_void,
    buffer: *mut u8,
    byte_count: *mut usize,
    client_data: *mut c_void,
) -> c_int {
    let outcome = catch_unwind(AssertUnwindSafe(|| unsafe {
        flac_read_callback_inner(buffer, byte_count, client_data)
    }));
    match outcome {
        Ok(status) => status,
        Err(_) => {
            record_callback_panic(client_data, "panic in libFLAC read callback");
            READ_ABORT
        }
    }
}

unsafe fn flac_read_callback_inner(
    buffer: *mut u8,
    byte_count: *mut usize,
    client_data: *mut c_void,
) -> c_int {
    if client_data.is_null() || byte_count.is_null() {
        return READ_ABORT;
    }
    // SAFETY: libFLAC passes the same non-null context pointer synchronously
    // from init_stream, and the context remains alive until processing ends.
    let context = unsafe { &mut *client_data.cast::<DecodeContext<'_, '_>>() };
    let requested = unsafe { *byte_count };
    unsafe { *byte_count = 0 };
    if context.controls.cancelled.load(Ordering::Relaxed) {
        context.callback_error = Some("conversion cancelled".to_owned());
        return READ_ABORT;
    }
    if requested == 0 {
        context.callback_error = Some("libFLAC requested a zero-length read".to_owned());
        return READ_ABORT;
    }
    if buffer.is_null() {
        context.callback_error = Some("libFLAC supplied a null input buffer".to_owned());
        return READ_ABORT;
    }
    // SAFETY: the libFLAC read callback contract gives `buffer` writable space
    // for the requested byte count.
    let target = unsafe { std::slice::from_raw_parts_mut(buffer, requested) };
    match context.input.read(target) {
        Ok(0) => READ_END_OF_STREAM,
        Ok(read) => {
            unsafe { *byte_count = read };
            READ_CONTINUE
        }
        Err(error) => {
            context.callback_error = Some(format!("reading FLAC source: {error}"));
            READ_ABORT
        }
    }
}

unsafe extern "C" fn flac_write_callback(
    _decoder: *const c_void,
    frame: *const c_void,
    buffers: *const *const i32,
    client_data: *mut c_void,
) -> c_int {
    let outcome = catch_unwind(AssertUnwindSafe(|| unsafe {
        flac_write_callback_inner(frame, buffers, client_data)
    }));
    match outcome {
        Ok(status) => status,
        Err(_) => {
            record_callback_panic(client_data, "panic in libFLAC frame callback");
            WRITE_ABORT
        }
    }
}

unsafe fn flac_write_callback_inner(
    frame: *const c_void,
    buffers: *const *const i32,
    client_data: *mut c_void,
) -> c_int {
    if client_data.is_null() || frame.is_null() || buffers.is_null() {
        return WRITE_ABORT;
    }
    // SAFETY: libFLAC calls this synchronously with the context and decoded
    // frame storage supplied by its documented write callback contract.
    let context = unsafe { &mut *client_data.cast::<DecodeContext<'_, '_>>() };
    if context.controls.cancelled.load(Ordering::Relaxed) {
        context.callback_error = Some("conversion cancelled".to_owned());
        return WRITE_ABORT;
    }
    // FLAC__Frame begins with FLAC__FrameHeader. The local repr(C) header
    // mirrors the public libFLAC header fields through the trailing CRC byte.
    let header = unsafe { &*frame.cast::<FlacFrameHeader>() };
    let block_frames = header.blocksize as usize;
    if block_frames == 0 || block_frames > PCM_BLOCK_LIMIT {
        context.callback_error = Some(format!("invalid FLAC block size {}", header.blocksize));
        return WRITE_ABORT;
    }
    if header.sample_rate != context.expected.sample_rate
        || header.channels != context.expected.channels
        || header.bits_per_sample != context.expected.bits_per_sample
    {
        context.callback_error = Some(format!(
            "FLAC frame format {} Hz, {} channels, {} bits differs from declared input format {} Hz, {} channels, {} bits",
            header.sample_rate,
            header.channels,
            header.bits_per_sample,
            context.expected.sample_rate,
            context.expected.channels,
            context.expected.bits_per_sample
        ));
        return WRITE_ABORT;
    }
    let channels = context.expected.channels as usize;
    let sample_count = match block_frames.checked_mul(channels) {
        Some(count) => count,
        None => {
            context.callback_error = Some("FLAC frame sample count overflow".to_owned());
            return WRITE_ABORT;
        }
    };
    let mut pcm = Vec::<i32>::new();
    if let Err(error) = pcm.try_reserve_exact(sample_count) {
        context.callback_error = Some(format!("allocating bounded FLAC frame buffer: {error}"));
        return WRITE_ABORT;
    }
    pcm.resize(sample_count, 0);

    for frame_index in 0..block_frames {
        for channel in 0..channels {
            // SAFETY: libFLAC supplies one valid channel pointer per header
            // channel and each contains `blocksize` signed decoded samples.
            let channel_buffer = unsafe { *buffers.add(channel) };
            if channel_buffer.is_null() {
                context.callback_error = Some("libFLAC supplied a null channel buffer".to_owned());
                return WRITE_ABORT;
            }
            let sample = unsafe { *channel_buffer.add(frame_index) };
            let Some(full_scale) = full_scale_sample(sample, context.expected.bits_per_sample)
            else {
                context.callback_error =
                    Some("libFLAC decoded a sample outside its declared precision".to_owned());
                return WRITE_ABORT;
            };
            let index = frame_index * channels + channel;
            pcm[index] = full_scale;
        }
    }
    update_pcm_hash(&mut context.hasher, &pcm);

    let next_frames = match context.frames.checked_add(block_frames as u64) {
        Some(frames) if frames <= context.expected.frames => frames,
        Some(frames) => {
            context.callback_error = Some(format!(
                "FLAC decoded at least {frames} frames, exceeding declared total {}",
                context.expected.frames
            ));
            return WRITE_ABORT;
        }
        None => {
            context.callback_error = Some("FLAC decoded frame count overflow".to_owned());
            return WRITE_ABORT;
        }
    };
    let packed = match packed_pcm_bytes(&pcm, context.output_bits) {
        Ok(bytes) => bytes,
        Err(error) => {
            context.callback_error = Some(format!("packing ALAC PCM block: {error:#}"));
            return WRITE_ABORT;
        }
    };
    let byte_size = match u32::try_from(packed.len()) {
        Ok(size) => size,
        Err(_) => {
            context.callback_error =
                Some("packed ALAC PCM block byte size exceeds AudioToolbox limit".to_owned());
            return WRITE_ABORT;
        }
    };
    let buffers = AudioBufferListOne {
        number_buffers: 1,
        first: AudioBuffer {
            number_channels: context.expected.channels,
            data_byte_size: byte_size,
            data: packed.as_ptr() as *mut c_void,
        },
    };
    let status = unsafe { ExtAudioFileWrite(context.writer, header.blocksize, &buffers) };
    if status != 0 {
        context.callback_error = Some(format!(
            "AudioToolbox ALAC write failed with OSStatus {status} (0x{status:08x})"
        ));
        return WRITE_ABORT;
    }
    context.frames = next_frames;
    if let Err(error) = (context.controls.progress)(next_frames) {
        context.callback_error = Some(format!("reporting conversion progress: {error:#}"));
        return WRITE_ABORT;
    }
    WRITE_CONTINUE
}

unsafe extern "C" fn flac_error_callback(
    _decoder: *const c_void,
    status: c_int,
    client_data: *mut c_void,
) {
    if client_data.is_null() {
        return;
    }
    // SAFETY: libFLAC calls this synchronously with the context supplied to
    // init_stream; no Rust references to it are active during the callback.
    let context = unsafe { &mut *client_data.cast::<DecodeContext<'_, '_>>() };
    context.decoder_error.get_or_insert(status);
}

fn record_callback_panic(client_data: *mut c_void, message: &str) {
    if client_data.is_null() {
        return;
    }
    // SAFETY: callback panics are caught before returning to libFLAC, and the
    // decoder invokes these callbacks synchronously while the context lives.
    let context = unsafe { &mut *client_data.cast::<DecodeContext<'_, '_>>() };
    context.callback_error = Some(message.to_owned());
}

struct FlacDecoder {
    pointer: *mut c_void,
    initialized: bool,
}

impl FlacDecoder {
    fn finish(&mut self) -> bool {
        if !self.initialized {
            return true;
        }
        self.initialized = false;
        unsafe { FLAC__stream_decoder_finish(self.pointer) != 0 }
    }
}

impl Drop for FlacDecoder {
    fn drop(&mut self) {
        if self.initialized {
            let _ = self.finish();
        }
        if !self.pointer.is_null() {
            unsafe { FLAC__stream_decoder_delete(self.pointer) };
            self.pointer = ptr::null_mut();
        }
    }
}

struct ExtAudioFileGuard(ExtAudioFileRef);

impl ExtAudioFileGuard {
    fn finish(&mut self) -> Result<()> {
        if self.0.is_null() {
            return Ok(());
        }
        let pointer = std::mem::replace(&mut self.0, ptr::null_mut());
        let status = unsafe { ExtAudioFileDispose(pointer) };
        check_os_status(status, "finalizing AudioToolbox file")
    }
}

impl Drop for ExtAudioFileGuard {
    fn drop(&mut self) {
        if !self.0.is_null() {
            unsafe { ExtAudioFileDispose(self.0) };
            self.0 = ptr::null_mut();
        }
    }
}

struct CfUrl(CfUrlRef);

impl Drop for CfUrl {
    fn drop(&mut self) {
        if !self.0.is_null() {
            unsafe { CFRelease(self.0) };
            self.0 = ptr::null();
        }
    }
}

/// Guards object identity and length only. File Provider hydration and benign
/// metadata changes may update time, mode, or xattrs; the engine separately
/// verifies source content with its pinned full-file SHA-256.
#[derive(Debug)]
struct FileSnapshot {
    dev: u64,
    ino: u64,
    len: u64,
    is_regular: bool,
}

impl PartialEq for FileSnapshot {
    fn eq(&self, other: &Self) -> bool {
        self.dev == other.dev
            && self.ino == other.ino
            && self.len == other.len
            && self.is_regular == other.is_regular
    }
}

impl Eq for FileSnapshot {}

impl FileSnapshot {
    fn read(file: &File) -> Result<Self> {
        let metadata = file
            .metadata()
            .context("reading file descriptor metadata")?;
        Ok(Self {
            dev: metadata.dev(),
            ino: metadata.ino(),
            len: metadata.len(),
            is_regular: metadata.file_type().is_file(),
        })
    }
}

fn open_input(path: &Path) -> Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
        .with_context(|| format!("opening FLAC input {} read-only", path.display()))?;
    ensure!(
        FileSnapshot::read(&file)?.is_regular,
        "FLAC input must be a regular file"
    );
    Ok(file)
}

fn open_wav_input(path: &Path) -> Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
        .with_context(|| format!("opening WAVE input {} read-only", path.display()))?;
    ensure!(
        FileSnapshot::read(&file)?.is_regular,
        "WAVE input must be a regular file"
    );
    Ok(file)
}

fn prepare_new_output(path: &Path) -> Result<PathBuf> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let parent = fs::canonicalize(parent).context("resolving ALAC output directory")?;
    ensure_private_directory(&parent)?;
    let name = path.file_name().context("ALAC output must name a file")?;
    let output = parent.join(name);
    match fs::symlink_metadata(&output) {
        Ok(_) => bail!("ALAC output already exists; conversion never overwrites files"),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error).context("checking ALAC output path"),
    }
    Ok(output)
}

fn canonical_output_path(path: &Path) -> Result<PathBuf> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let parent = fs::canonicalize(parent).context("resolving ALAC file directory")?;
    ensure_private_directory(&parent)?;
    let name = path.file_name().context("ALAC output must name a file")?;
    Ok(parent.join(name))
}

fn ensure_private_directory(path: &Path) -> Result<()> {
    let metadata =
        fs::metadata(path).with_context(|| format!("reading directory {}", path.display()))?;
    ensure!(metadata.is_dir(), "ALAC output parent must be a directory");
    ensure!(
        metadata.uid() == unsafe { libc::geteuid() },
        "ALAC output directory must be owned by the current user"
    );
    ensure!(
        metadata.mode() & 0o077 == 0,
        "ALAC output directory must not be accessible by group or other users"
    );
    Ok(())
}

fn secure_output_handle(path: &Path) -> Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
        .context("opening newly created ALAC file without following symlinks")?;
    ensure!(
        FileSnapshot::read(&file)?.is_regular,
        "AudioToolbox output must be a regular file"
    );
    file.set_permissions(Permissions::from_mode(0o600))
        .context("setting private ALAC output permissions")?;
    Ok(file)
}

fn open_existing_output(path: &Path) -> Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
        .with_context(|| format!("opening ALAC output {} read-only", path.display()))?;
    let snapshot = FileSnapshot::read(&file)?;
    ensure!(
        snapshot.is_regular,
        "ALAC verification input must be a regular file"
    );
    let metadata = file.metadata().context("reading ALAC output permissions")?;
    ensure!(
        metadata.mode() & 0o077 == 0,
        "ALAC output file must be private during verification"
    );
    Ok(file)
}

fn file_url(path: &Path) -> Result<CfUrl> {
    use std::os::unix::ffi::OsStrExt;
    let bytes = path.as_os_str().as_bytes();
    let length =
        isize::try_from(bytes.len()).context("file path is too long for CoreFoundation")?;
    let url =
        unsafe { CFURLCreateFromFileSystemRepresentation(ptr::null(), bytes.as_ptr(), length, 0) };
    ensure!(!url.is_null(), "CoreFoundation could not create a file URL");
    Ok(CfUrl(url))
}

fn create_alac_writer(
    url: CfUrlRef,
    spec: &AudioSpec,
    output_bits: u32,
) -> Result<ExtAudioFileGuard> {
    let output_format = AudioStreamBasicDescription {
        sample_rate: f64::from(spec.sample_rate),
        format_id: AUDIO_FORMAT_ALAC,
        format_flags: alac_flags(output_bits)?,
        frames_per_packet: 4_096,
        channels_per_frame: spec.channels,
        ..AudioStreamBasicDescription::default()
    };
    let mut file = ptr::null_mut();
    let status = unsafe {
        ExtAudioFileCreateWithURL(
            url,
            AUDIO_FILE_M4A,
            &output_format,
            ptr::null(),
            0,
            &mut file,
        )
    };
    let guard = ExtAudioFileGuard(file);
    check_os_status(status, "creating new ALAC M4A file")?;
    ensure!(
        !guard.0.is_null(),
        "AudioToolbox created no ALAC file handle"
    );
    let client_format = pcm_source_format(spec.sample_rate, spec.channels, output_bits)?;
    let status = unsafe {
        ExtAudioFileSetProperty(
            guard.0,
            EXT_CLIENT_DATA_FORMAT,
            size_of::<AudioStreamBasicDescription>() as u32,
            (&client_format as *const AudioStreamBasicDescription).cast(),
        )
    };
    check_os_status(status, "configuring AudioToolbox PCM input format")?;
    Ok(guard)
}

fn open_alac_reader(url: CfUrlRef) -> Result<ExtAudioFileGuard> {
    let mut file = ptr::null_mut();
    let status = unsafe { ExtAudioFileOpenURL(url, &mut file) };
    let guard = ExtAudioFileGuard(file);
    check_os_status(status, "opening ALAC M4A for verification")?;
    ensure!(
        !guard.0.is_null(),
        "AudioToolbox opened no ALAC file handle"
    );
    Ok(guard)
}

fn get_file_format(file: ExtAudioFileRef) -> Result<AudioStreamBasicDescription> {
    let mut format = AudioStreamBasicDescription::default();
    let mut size = size_of::<AudioStreamBasicDescription>() as u32;
    let status = unsafe {
        ExtAudioFileGetProperty(
            file,
            EXT_FILE_DATA_FORMAT,
            &mut size,
            (&mut format as *mut AudioStreamBasicDescription).cast(),
        )
    };
    check_os_status(status, "reading ALAC output format")?;
    ensure!(
        size == size_of::<AudioStreamBasicDescription>() as u32,
        "AudioToolbox returned an unexpected ALAC format description size"
    );
    Ok(format)
}

fn get_file_frames(file: ExtAudioFileRef) -> Result<u64> {
    let mut frames = 0_i64;
    let mut size = size_of::<i64>() as u32;
    let status = unsafe {
        ExtAudioFileGetProperty(
            file,
            EXT_FILE_LENGTH_FRAMES,
            &mut size,
            (&mut frames as *mut i64).cast(),
        )
    };
    check_os_status(status, "reading ALAC frame count")?;
    ensure!(
        size == size_of::<i64>() as u32,
        "AudioToolbox returned an unexpected ALAC frame count size"
    );
    ensure!(
        frames >= 0,
        "AudioToolbox returned a negative ALAC frame count"
    );
    Ok(frames as u64)
}

fn inspect_output_header(path: &Path, expected_bits: u32) -> Result<AudioSpec> {
    let path = canonical_output_path(path)?;
    let _file_guard = open_existing_output(&path)?;
    let url = file_url(&path)?;
    let mut reader = open_alac_reader(url.0)?;
    let format = get_file_format(reader.0)?;
    let frames = get_file_frames(reader.0)?;
    let spec = spec_from_file_format(format, frames)?;
    ensure!(
        spec.bits_per_sample == expected_bits,
        "AudioToolbox ALAC header reports {}-bit source precision, expected {expected_bits}",
        spec.bits_per_sample
    );
    reader.finish()?;
    Ok(spec)
}

fn spec_from_file_format(format: AudioStreamBasicDescription, frames: u64) -> Result<AudioSpec> {
    ensure!(
        format.format_id == AUDIO_FORMAT_ALAC,
        "output is not Apple Lossless audio"
    );
    ensure!(
        format.sample_rate.is_finite()
            && format.sample_rate > 0.0
            && format.sample_rate.fract() == 0.0
            && format.sample_rate <= f64::from(u32::MAX),
        "ALAC output has an unsupported sample rate"
    );
    let bits_per_sample = match format.format_flags {
        ALAC_DEPTH_16 => 16,
        ALAC_DEPTH_20 => 20,
        ALAC_DEPTH_24 => 24,
        ALAC_DEPTH_32 => 32,
        flags => bail!("ALAC header has unknown source-precision flags 0x{flags:08x}"),
    };
    ensure!(
        (1..=2).contains(&format.channels_per_frame),
        "ALAC output has an unsupported channel layout; this version validates mono and stereo only"
    );
    Ok(AudioSpec {
        sample_rate: format.sample_rate as u32,
        channels: format.channels_per_frame,
        bits_per_sample,
        frames,
    })
}

fn set_pcm32_client_format(file: ExtAudioFileRef, spec: &AudioSpec) -> Result<()> {
    let format = pcm32_format(spec.sample_rate, spec.channels)?;
    let status = unsafe {
        ExtAudioFileSetProperty(
            file,
            EXT_CLIENT_DATA_FORMAT,
            size_of::<AudioStreamBasicDescription>() as u32,
            (&format as *const AudioStreamBasicDescription).cast(),
        )
    };
    check_os_status(status, "configuring ALAC PCM verification format")
}

fn pcm32_format(sample_rate: u32, channels: u32) -> Result<AudioStreamBasicDescription> {
    let bytes_per_frame = channels
        .checked_mul(size_of::<i32>() as u32)
        .context("PCM32 frame byte size overflow")?;
    Ok(AudioStreamBasicDescription {
        sample_rate: f64::from(sample_rate),
        format_id: AUDIO_FORMAT_LPCM,
        format_flags: AUDIO_FORMAT_FLAG_SIGNED_INTEGER | AUDIO_FORMAT_FLAG_PACKED,
        bytes_per_packet: bytes_per_frame,
        frames_per_packet: 1,
        bytes_per_frame,
        channels_per_frame: channels,
        bits_per_channel: 32,
        reserved: 0,
    })
}

fn pcm_source_format(
    sample_rate: u32,
    channels: u32,
    valid_bits: u32,
) -> Result<AudioStreamBasicDescription> {
    let container_bits = match valid_bits {
        16 => 16,
        20 | 24 => 24,
        32 => 32,
        _ => bail!("unsupported ALAC PCM source depth {valid_bits}"),
    };
    let bytes_per_frame = channels
        .checked_mul(container_bits / 8)
        .context("ALAC PCM source frame byte size overflow")?;
    let format_flags = AUDIO_FORMAT_FLAG_SIGNED_INTEGER
        | (if container_bits == valid_bits {
            AUDIO_FORMAT_FLAG_PACKED
        } else {
            AUDIO_FORMAT_FLAG_ALIGNED_HIGH
        });
    Ok(AudioStreamBasicDescription {
        sample_rate: f64::from(sample_rate),
        format_id: AUDIO_FORMAT_LPCM,
        format_flags,
        bytes_per_packet: bytes_per_frame,
        frames_per_packet: 1,
        bytes_per_frame,
        channels_per_frame: channels,
        bits_per_channel: valid_bits,
        reserved: 0,
    })
}

fn packed_pcm_bytes(samples: &[i32], valid_bits: u32) -> Result<Vec<u8>> {
    let container_bits = match valid_bits {
        16 => 16,
        20 | 24 => 24,
        32 => 32,
        _ => bail!("unsupported ALAC PCM source depth {valid_bits}"),
    };
    let bytes_per_sample = usize::try_from(container_bits / 8)
        .context("ALAC PCM sample width does not fit this platform")?;
    let byte_count = samples
        .len()
        .checked_mul(bytes_per_sample)
        .context("packed ALAC PCM byte count overflow")?;
    let mut packed = Vec::new();
    packed
        .try_reserve_exact(byte_count)
        .context("allocating bounded packed ALAC PCM block")?;
    let shift = 32 - container_bits;
    for sample in samples {
        let value = sample >> shift;
        packed.extend_from_slice(&value.to_le_bytes()[..bytes_per_sample]);
    }
    Ok(packed)
}

fn alac_flags(bits: u32) -> Result<u32> {
    match bits {
        16 => Ok(ALAC_DEPTH_16),
        20 => Ok(ALAC_DEPTH_20),
        24 => Ok(ALAC_DEPTH_24),
        32 => Ok(ALAC_DEPTH_32),
        _ => bail!("unsupported ALAC source precision {bits}"),
    }
}

fn check_cancelled(controls: &Controls<'_>) -> Result<()> {
    if controls.cancelled.load(Ordering::Relaxed) {
        bail!("conversion cancelled");
    }
    Ok(())
}

fn check_os_status(status: i32, action: &str) -> Result<()> {
    if status != 0 {
        bail!("{action} failed with OSStatus {status} (0x{status:08x})");
    }
    Ok(())
}

fn hex_digest(digest: [u8; 32]) -> String {
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn update_pcm_hash(hasher: &mut Sha256, samples: &[i32]) {
    #[cfg(target_endian = "little")]
    {
        // SAFETY: i32 has no padding and all samples are initialized. This
        // target is little-endian, so the bytes are canonical signed32 LE.
        let bytes = unsafe {
            std::slice::from_raw_parts(
                samples.as_ptr().cast::<u8>(),
                std::mem::size_of_val(samples),
            )
        };
        hasher.update(bytes);
    }
    #[cfg(not(target_endian = "little"))]
    for sample in samples {
        hasher.update(sample.to_le_bytes());
    }
}

const fn fourcc(bytes: [u8; 4]) -> u32 {
    u32::from_be_bytes(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn file_shape_guard_ignores_permission_metadata_but_detects_size_changes() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("fixture.flac");
        fs::write(&path, b"fixture").unwrap();
        fs::set_permissions(&path, Permissions::from_mode(0o644)).unwrap();
        let file = File::open(&path).unwrap();
        let before = FileSnapshot::read(&file).unwrap();

        fs::set_permissions(&path, Permissions::from_mode(0o600)).unwrap();
        let after_metadata_change = FileSnapshot::read(&file).unwrap();
        assert_eq!(before, after_metadata_change);

        OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(b"!")
            .unwrap();
        let after_size_change = FileSnapshot::read(&file).unwrap();
        assert_ne!(before, after_size_change);
    }

    #[test]
    fn alac_header_validation_requires_alac_and_supported_source_depth() {
        let base = AudioStreamBasicDescription {
            sample_rate: 48_000.0,
            format_id: AUDIO_FORMAT_ALAC,
            format_flags: ALAC_DEPTH_32,
            channels_per_frame: 2,
            ..AudioStreamBasicDescription::default()
        };
        assert_eq!(
            spec_from_file_format(base, 123).unwrap().bits_per_sample,
            32
        );

        let mut wrong_codec = base;
        wrong_codec.format_id = AUDIO_FORMAT_LPCM;
        assert!(
            spec_from_file_format(wrong_codec, 123)
                .unwrap_err()
                .to_string()
                .contains("not Apple Lossless")
        );

        let mut unknown_depth = base;
        unknown_depth.format_flags = 5;
        assert!(
            spec_from_file_format(unknown_depth, 123)
                .unwrap_err()
                .to_string()
                .contains("unknown source-precision flags")
        );
    }

    #[test]
    fn pcm_client_description_uses_full_scale_signed_32_bit_samples() {
        let format = pcm32_format(96_000, 2).unwrap();
        assert_eq!(format.format_id, AUDIO_FORMAT_LPCM);
        assert_eq!(
            format.format_flags,
            AUDIO_FORMAT_FLAG_SIGNED_INTEGER | AUDIO_FORMAT_FLAG_PACKED
        );
        assert_eq!(format.bits_per_channel, 32);
        assert_eq!(format.bytes_per_frame, 8);
        assert_eq!(format.bytes_per_packet, 8);
        assert_eq!(full_scale_sample(i32::MIN, 32), Some(i32::MIN));
        assert_eq!(full_scale_sample(i32::MAX, 32), Some(i32::MAX));
    }

    #[test]
    fn alac_client_formats_and_packing_match_source_precision() {
        let pcm16 = pcm_source_format(48_000, 2, 16).unwrap();
        assert_eq!(pcm16.bits_per_channel, 16);
        assert_eq!(pcm16.bytes_per_frame, 4);
        assert_eq!(
            pcm16.format_flags,
            AUDIO_FORMAT_FLAG_SIGNED_INTEGER | AUDIO_FORMAT_FLAG_PACKED
        );
        assert_eq!(
            packed_pcm_bytes(&[i32::MIN, 2_147_418_112], 16).unwrap(),
            vec![0x00, 0x80, 0xff, 0x7f]
        );

        let pcm20 = pcm_source_format(48_000, 2, 20).unwrap();
        assert_eq!(pcm20.bits_per_channel, 20);
        assert_eq!(pcm20.bytes_per_frame, 6);
        assert_eq!(
            pcm20.format_flags,
            AUDIO_FORMAT_FLAG_SIGNED_INTEGER | AUDIO_FORMAT_FLAG_ALIGNED_HIGH
        );
        assert_eq!(
            packed_pcm_bytes(&[i32::MIN, 2_147_479_552], 20).unwrap(),
            vec![0x00, 0x00, 0x80, 0xf0, 0xff, 0x7f]
        );

        let pcm24 = pcm_source_format(48_000, 2, 24).unwrap();
        assert_eq!(pcm24.bits_per_channel, 24);
        assert_eq!(pcm24.bytes_per_frame, 6);
        assert_eq!(
            pcm24.format_flags,
            AUDIO_FORMAT_FLAG_SIGNED_INTEGER | AUDIO_FORMAT_FLAG_PACKED
        );
        assert_eq!(
            packed_pcm_bytes(&[i32::MIN, 2_147_483_392], 24).unwrap(),
            vec![0x00, 0x00, 0x80, 0xff, 0xff, 0x7f]
        );

        let pcm32 = pcm_source_format(48_000, 2, 32).unwrap();
        let pcm32_verify = pcm32_format(48_000, 2).unwrap();
        assert_eq!(pcm32.bits_per_channel, pcm32_verify.bits_per_channel);
        assert_eq!(pcm32.format_flags, pcm32_verify.format_flags);
        assert_eq!(pcm32.bytes_per_frame, pcm32_verify.bytes_per_frame);
        assert_eq!(
            packed_pcm_bytes(&[i32::MIN, i32::MAX], 32).unwrap(),
            vec![0x00, 0x00, 0x00, 0x80, 0xff, 0xff, 0xff, 0x7f]
        );
    }
}
