use pgrx::prelude::*;

use crate::mem_io::{MemInput, MemOutput};
use crate::pipeline;

use ffmpeg_next::codec;
use ffmpeg_next::media::Type;
use ffmpeg_next::Rational;

#[pg_extern]
fn trim(
    data: Vec<u8>,
    start_time: default!(f64, 0.0),
    end_time: default!(Option<f64>, "NULL"),
    precise: default!(bool, false),
) -> Vec<u8> {
    ffmpeg_next::init().unwrap();
    validate_trim_args(start_time, end_time);

    if precise {
        precise_trim(data, start_time, end_time)
    } else {
        fast_trim(data, start_time, end_time)
    }
}

fn validate_trim_args(start_time: f64, end_time: Option<f64>) {
    if start_time < 0.0 {
        error!("pg_ffmpeg: start_time must be >= 0");
    }
    if let Some(end_time) = end_time {
        if end_time <= start_time {
            error!("pg_ffmpeg: end_time must be > start_time");
        }
    }
}

fn fast_trim(data: Vec<u8>, start_time: f64, end_time: Option<f64>) -> Vec<u8> {
    let mut ictx = MemInput::open(&data);
    let out_format = default_output_format(ictx.format().name());
    let trim_duration = end_time.map(|end| end - start_time);

    let anchor_stream_index = ictx
        .streams()
        .best(Type::Video)
        .or_else(|| ictx.streams().best(Type::Audio))
        .map(|stream| stream.index());

    let mut octx = MemOutput::open(&out_format);
    let mut stream_mapping = vec![None::<usize>; ictx.streams().count()];
    let mut input_time_bases = vec![Rational(0, 0); ictx.streams().count()];
    for stream in ictx.streams() {
        let input_index = stream.index();
        input_time_bases[input_index] = stream.time_base();
        stream_mapping[input_index] = Some(pipeline::copy_stream(&stream, &mut octx));
    }

    octx.write_header()
        .unwrap_or_else(|e| error!("failed to write trim header: {e}"));

    let mut output_time_bases = vec![Rational(0, 0); octx.streams().count()];
    for (index, _) in octx.streams().enumerate() {
        output_time_bases[index] = octx.stream(index).unwrap().time_base();
    }

    let mut skipped_anchor_duration = 0.0f64;
    let mut copied_anchor_duration = 0.0f64;
    let mut started = start_time <= 0.0;

    for (stream, mut packet) in ictx.packets() {
        let input_index = stream.index();
        let Some(output_index) = stream_mapping[input_index] else {
            continue;
        };

        let is_anchor = Some(input_index) == anchor_stream_index;
        let packet_duration_seconds = if is_anchor {
            packet_duration_seconds(packet.duration(), stream.time_base())
        } else {
            None
        };

        if !started {
            if !is_anchor {
                continue;
            }
            let duration = packet_duration_seconds.unwrap_or(0.0);
            if skipped_anchor_duration + duration < start_time {
                skipped_anchor_duration += duration;
                continue;
            }
            started = true;
        }

        if is_anchor {
            if let Some(limit) = trim_duration {
                if copied_anchor_duration >= limit {
                    break;
                }
            }
        }

        let input_time_base = input_time_bases[input_index];
        let output_time_base = output_time_bases[output_index];
        packet.set_stream(output_index);
        packet.rescale_ts(input_time_base, output_time_base);
        packet.set_position(-1);
        packet
            .write_interleaved(&mut octx)
            .unwrap_or_else(|e| error!("failed to write trimmed packet: {e}"));

        if is_anchor {
            copied_anchor_duration += packet_duration_seconds.unwrap_or(0.0);
        }
    }

    octx.write_trailer()
        .unwrap_or_else(|e| error!("failed to write trim trailer: {e}"));
    octx.into_data()
}

fn precise_trim(data: Vec<u8>, start_time: f64, end_time: Option<f64>) -> Vec<u8> {
    let (out_format, has_video, has_audio, dropped_aux_streams) = {
        let ictx = MemInput::open(&data);
        let out_format = default_output_format(ictx.format().name());
        let has_video = ictx.streams().best(Type::Video).is_some();
        let has_audio = ictx.streams().best(Type::Audio).is_some();
        let dropped_aux_streams = ictx
            .streams()
            .filter(|stream| matches!(stream.parameters().medium(), Type::Subtitle | Type::Data))
            .count();
        (out_format, has_video, has_audio, dropped_aux_streams)
    };

    if !has_video && !has_audio {
        error!("pg_ffmpeg: trim found no audio or video streams to keep");
    }

    let transcode_input = if dropped_aux_streams > 0 {
        pgrx::warning!(
            "pg_ffmpeg: trim(precise=true) dropped {} subtitle/data stream(s); use precise=false to preserve them",
            dropped_aux_streams
        );
        remux_audio_video_only(&data, &out_format)
    } else {
        data
    };

    let (video_base_time, audio_base_time) = first_stream_timestamp_bases(&transcode_input);
    let input_audio_codec_id = {
        let input = MemInput::open(&transcode_input);
        input
            .streams()
            .best(Type::Audio)
            .map(|stream| stream.parameters().id())
    };
    let (video_codec, mut audio_codec) = precise_codec_names(&transcode_input);
    if out_format == "mpegts" && input_audio_codec_id == Some(codec::Id::MP3) {
        audio_codec = Some("mp2".to_owned());
    }
    let video_filter =
        has_video.then(|| video_trim_filter(video_base_time.unwrap_or(0.0), start_time, end_time));
    let audio_filter =
        has_audio.then(|| audio_trim_filter(audio_base_time.unwrap_or(0.0), start_time, end_time));

    crate::transcode::transcode(
        transcode_input,
        Some(&out_format),
        video_filter.as_deref(),
        video_codec.as_deref(),
        None,
        None,
        None,
        audio_codec.as_deref(),
        audio_filter.as_deref(),
        None,
        false,
    )
}

fn default_output_format(input_format: &str) -> String {
    match input_format {
        "png_pipe" | "ppm_pipe" => "image2pipe".to_owned(),
        _ => input_format.to_owned(),
    }
}

fn video_trim_filter(base_time: f64, start_time: f64, end_time: Option<f64>) -> String {
    let start = base_time + start_time;
    match end_time {
        Some(end_time) => format!(
            "trim=start={start}:end={},setpts=PTS-STARTPTS",
            base_time + end_time
        ),
        None => format!("trim=start={start},setpts=PTS-STARTPTS"),
    }
}

fn audio_trim_filter(base_time: f64, start_time: f64, end_time: Option<f64>) -> String {
    let start = base_time + start_time;
    match end_time {
        Some(end_time) => format!(
            "atrim=start={start}:end={},asetpts=PTS-STARTPTS",
            base_time + end_time
        ),
        None => format!("atrim=start={start},asetpts=PTS-STARTPTS"),
    }
}

fn remux_audio_video_only(data: &[u8], out_format: &str) -> Vec<u8> {
    let mut ictx = MemInput::open(data);
    let mut octx = MemOutput::open(out_format);

    let mut stream_mapping = vec![None::<usize>; ictx.streams().count()];
    let mut input_time_bases = vec![Rational(0, 0); ictx.streams().count()];
    let mut kept_streams = 0usize;

    for stream in ictx.streams() {
        let input_index = stream.index();
        input_time_bases[input_index] = stream.time_base();
        match stream.parameters().medium() {
            Type::Video | Type::Audio => {
                stream_mapping[input_index] = Some(pipeline::copy_stream(&stream, &mut octx));
                kept_streams += 1;
            }
            _ => {}
        }
    }

    if kept_streams == 0 {
        error!("pg_ffmpeg: trim found no audio or video streams to keep");
    }

    octx.write_header()
        .unwrap_or_else(|e| error!("failed to write trim header: {e}"));

    for (stream, mut packet) in ictx.packets() {
        let input_index = stream.index();
        let Some(output_index) = stream_mapping[input_index] else {
            continue;
        };
        let output_time_base = octx.stream(output_index).unwrap().time_base();
        packet.set_stream(output_index);
        packet.rescale_ts(input_time_bases[input_index], output_time_base);
        packet.set_position(-1);
        packet
            .write_interleaved(&mut octx)
            .unwrap_or_else(|e| error!("failed to write remuxed packet: {e}"));
    }

    octx.write_trailer()
        .unwrap_or_else(|e| error!("failed to write trim trailer: {e}"));
    octx.into_data()
}

fn timestamp_to_seconds(timestamp: i64, time_base: Rational) -> f64 {
    if time_base.denominator() == 0 {
        0.0
    } else {
        timestamp as f64 * f64::from(time_base.numerator()) / f64::from(time_base.denominator())
    }
}

fn packet_duration_seconds(duration: i64, time_base: Rational) -> Option<f64> {
    (duration > 0).then(|| timestamp_to_seconds(duration, time_base))
}

fn precise_codec_names(data: &[u8]) -> (Option<String>, Option<String>) {
    let input = MemInput::open(data);

    let video_codec = input
        .streams()
        .best(Type::Video)
        .map(|stream| precise_codec_name(stream.parameters().id(), Type::Video, "libx264"));
    let audio_codec = input
        .streams()
        .best(Type::Audio)
        .map(|stream| precise_codec_name(stream.parameters().id(), Type::Audio, "aac"));

    (video_codec, audio_codec)
}

fn precise_codec_name(codec_id: codec::Id, medium: Type, fallback: &str) -> String {
    if let Some(codec) = codec::encoder::find(codec_id) {
        return codec.name().to_owned();
    }

    let medium_label = match medium {
        Type::Video => "video",
        Type::Audio => "audio",
        _ => "stream",
    };
    pgrx::warning!(
        "pg_ffmpeg: trim(precise=true) source {:?} has no encoder in this FFmpeg build; re-encoding {} as {}",
        codec_id,
        medium_label,
        fallback
    );
    fallback.to_owned()
}

fn first_stream_timestamp_bases(data: &[u8]) -> (Option<f64>, Option<f64>) {
    let mut input = MemInput::open(data);
    let video_stream_index = input
        .streams()
        .best(Type::Video)
        .map(|stream| stream.index());
    let audio_stream_index = input
        .streams()
        .best(Type::Audio)
        .map(|stream| stream.index());

    let video_time_base = video_stream_index.and_then(|index| {
        input
            .stream(index)
            .and_then(|stream| packet_timestamp_time_base(&stream))
    });
    let audio_time_base = audio_stream_index.and_then(|index| {
        input
            .stream(index)
            .and_then(|stream| packet_timestamp_time_base(&stream))
    });

    let mut first_video = None;
    let mut first_audio = None;
    for (stream, packet) in input.packets() {
        let packet_timestamp = packet.pts().or_else(|| packet.dts());
        let Some(timestamp) = packet_timestamp else {
            continue;
        };
        if Some(stream.index()) == video_stream_index && first_video.is_none() {
            if let Some(time_base) = video_time_base {
                first_video = Some(timestamp_to_seconds(timestamp, time_base));
            }
        }
        if Some(stream.index()) == audio_stream_index && first_audio.is_none() {
            if let Some(time_base) = audio_time_base {
                first_audio = Some(
                    timestamp_to_seconds(timestamp, time_base)
                        + audio_stream_start_offset_seconds(&stream),
                );
            }
        }
        if (video_stream_index.is_none() || first_video.is_some())
            && (audio_stream_index.is_none() || first_audio.is_some())
        {
            break;
        }
    }

    (first_video, first_audio)
}

fn packet_timestamp_time_base(stream: &ffmpeg_next::format::stream::Stream) -> Option<Rational> {
    match stream.parameters().medium() {
        Type::Video => video_packet_time_base(stream),
        Type::Audio => audio_packet_time_base(stream),
        _ => None,
    }
}

fn video_packet_time_base(stream: &ffmpeg_next::format::stream::Stream) -> Option<Rational> {
    let avg_frame_rate = stream.avg_frame_rate();
    if avg_frame_rate.numerator() > 0 && avg_frame_rate.denominator() > 0 {
        return Some(Rational(
            avg_frame_rate.denominator(),
            avg_frame_rate.numerator(),
        ));
    }

    let duration = stream.duration();
    let frames = stream.frames();
    let stream_time_base = stream.time_base();
    if duration > 0
        && frames > 0
        && stream_time_base.numerator() > 0
        && stream_time_base.denominator() > 0
    {
        let numerator = i64::from(stream_time_base.denominator()) * duration;
        let denominator = i64::from(stream_time_base.numerator()) * frames;
        if numerator > 0 && denominator > 0 {
            let gcd = gcd_i64(numerator, denominator);
            let reduced_num = numerator / gcd;
            let reduced_den = denominator / gcd;
            if let (Ok(num), Ok(den)) = (i32::try_from(reduced_num), i32::try_from(reduced_den)) {
                return Some(Rational(num, den));
            }
        }
    }

    let rate = stream.rate();
    if rate.numerator() > 0 && rate.denominator() > 0 {
        return Some(Rational(rate.denominator(), rate.numerator()));
    }

    None
}

fn audio_packet_time_base(stream: &ffmpeg_next::format::stream::Stream) -> Option<Rational> {
    let decoder_ctx =
        ffmpeg_next::codec::context::Context::from_parameters(stream.parameters()).ok()?;
    let decoder = decoder_ctx.decoder().audio().ok()?;
    (decoder.rate() > 0).then(|| Rational(1, decoder.rate() as i32))
}

fn audio_stream_start_offset_seconds(stream: &ffmpeg_next::format::stream::Stream) -> f64 {
    let start_time = stream.start_time();
    if start_time <= 0 {
        0.0
    } else {
        timestamp_to_seconds(start_time, stream.time_base())
    }
}

fn gcd_i64(mut a: i64, mut b: i64) -> i64 {
    while b != 0 {
        let r = a % b;
        a = b;
        b = r;
    }
    a.abs().max(1)
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use super::*;
    use crate::mem_io::MemInput;
    use crate::test_utils::{generate_test_video_bytes, generate_test_video_with_audio_bytes};
    use ffmpeg_next::util::frame::{audio::Audio as AudioFrame, video::Video as VideoFrame};
    use ffmpeg_next::Rescale;

    #[pg_test]
    fn test_trim_fast_keyframe() {
        let data = generate_test_video_bytes(64, 64, 10, 4);
        let result = trim(data, 1.0, Some(2.5), false);
        assert!(!result.is_empty(), "trim output should not be empty");

        let frame_count = decoded_video_frame_count(&result);
        assert!(
            (14..=16).contains(&frame_count),
            "fast trim should keep about 15 frames, got {frame_count}"
        );
    }

    #[pg_test]
    fn test_trim_precise_reencode() {
        let data = generate_test_video_bytes(64, 64, 10, 4);
        let result = trim(data, 1.0, Some(2.5), true);
        assert!(!result.is_empty(), "trim output should not be empty");

        let probe = MemInput::open(&result);
        assert!(probe.streams().best(Type::Video).is_some());
        let frame_count = decoded_video_frame_count(&result);
        assert!(
            (14..=16).contains(&frame_count),
            "precise trim should keep about 15 frames, got {frame_count}"
        );
    }

    #[pg_test]
    fn test_trim_to_end() {
        let data = generate_test_video_bytes(64, 64, 10, 4);
        let result = trim(data, 1.5, None, true);
        assert!(!result.is_empty(), "trim output should not be empty");

        let frame_count = decoded_video_frame_count(&result);
        assert!(
            (24..=26).contains(&frame_count),
            "trim-to-end should keep about 25 frames, got {frame_count}"
        );
    }

    #[pg_test]
    fn test_trim_av_sync_precise() {
        let data = generate_test_video_with_audio_bytes(64, 64, 10, 4);
        let result = trim(data, 1.0, Some(2.8), true);
        assert!(!result.is_empty(), "trim output should not be empty");

        let video_duration =
            decoded_video_duration_seconds(&result).expect("expected video output");
        let audio_duration =
            decoded_audio_duration_seconds(&result).expect("expected audio output");
        assert!(
            (video_duration - audio_duration).abs() <= 0.25,
            "precise trim should keep A/V durations closely aligned on encoded MPEG-TS output: video={video_duration}, audio={audio_duration}"
        );
    }

    fn decoded_video_frame_count(data: &[u8]) -> usize {
        let mut input = MemInput::open(data);
        let Some(stream) = input.streams().best(Type::Video) else {
            return 0;
        };
        let stream_index = stream.index();
        let decoder_ctx =
            ffmpeg_next::codec::context::Context::from_parameters(stream.parameters())
                .expect("failed to create video decoder context");
        let mut decoder = decoder_ctx
            .decoder()
            .video()
            .expect("failed to open video decoder");
        decoder.set_time_base(stream.time_base());

        let mut frame_count = 0usize;
        let mut frame = VideoFrame::empty();
        for (packet_stream, mut packet) in input.packets() {
            if packet_stream.index() != stream_index {
                continue;
            }
            packet.rescale_ts(packet_stream.time_base(), decoder.time_base());
            decoder
                .send_packet(&packet)
                .expect("failed to send video packet");
            while decoder.receive_frame(&mut frame).is_ok() {
                frame_count += 1;
            }
        }
        decoder.send_eof().expect("failed to flush video decoder");
        while decoder.receive_frame(&mut frame).is_ok() {
            frame_count += 1;
        }
        frame_count
    }

    fn decoded_video_duration_seconds(data: &[u8]) -> Option<f64> {
        let frame_count = decoded_video_frame_count(data);
        if frame_count == 0 {
            return None;
        }

        let input = MemInput::open(data);
        let stream = input.streams().best(Type::Video)?;
        let mut frame_rate = stream.avg_frame_rate();
        if frame_rate.numerator() <= 0 || frame_rate.denominator() <= 0 {
            frame_rate = stream.rate();
        }
        if frame_rate.numerator() <= 0 || frame_rate.denominator() <= 0 {
            let decoder_ctx =
                ffmpeg_next::codec::context::Context::from_parameters(stream.parameters()).ok()?;
            let decoder = decoder_ctx.decoder().video().ok()?;
            frame_rate = decoder.frame_rate()?;
        }
        if frame_rate.numerator() <= 0 || frame_rate.denominator() <= 0 {
            return None;
        }
        let fps = f64::from(frame_rate.numerator()) / f64::from(frame_rate.denominator());
        Some(frame_count as f64 / fps)
    }

    fn decoded_audio_duration_seconds(data: &[u8]) -> Option<f64> {
        let mut input = MemInput::open(data);
        let stream = input.streams().best(Type::Audio)?;
        let stream_index = stream.index();
        let decoder_ctx =
            ffmpeg_next::codec::context::Context::from_parameters(stream.parameters()).ok()?;
        let mut decoder = decoder_ctx.decoder().audio().ok()?;
        let frame_time_base = if decoder.rate() > 0 {
            Rational(1, decoder.rate() as i32)
        } else {
            decoder.time_base()
        };

        let mut first_pts = None::<i64>;
        let mut last_end_pts = None::<i64>;
        let mut next_pts = 0i64;
        let mut frame = AudioFrame::empty();
        for (packet_stream, mut packet) in input.packets() {
            if packet_stream.index() != stream_index {
                continue;
            }
            packet.rescale_ts(packet_stream.time_base(), decoder.time_base());
            decoder.send_packet(&packet).ok()?;
            while decoder.receive_frame(&mut frame).is_ok() {
                let timestamp = frame
                    .timestamp()
                    .or_else(|| frame.pts())
                    .map(|pts| pts.rescale(decoder.time_base(), frame_time_base))
                    .unwrap_or(next_pts);
                first_pts.get_or_insert(timestamp);
                next_pts = timestamp.saturating_add(frame.samples() as i64);
                last_end_pts = Some(next_pts);
            }
        }
        decoder.send_eof().ok()?;
        while decoder.receive_frame(&mut frame).is_ok() {
            let timestamp = frame
                .timestamp()
                .or_else(|| frame.pts())
                .map(|pts| pts.rescale(decoder.time_base(), frame_time_base))
                .unwrap_or(next_pts);
            first_pts.get_or_insert(timestamp);
            next_pts = timestamp.saturating_add(frame.samples() as i64);
            last_end_pts = Some(next_pts);
        }
        if decoder.rate() == 0 {
            None
        } else {
            let start_pts = first_pts.unwrap_or(0).max(0);
            Some(last_end_pts?.saturating_sub(start_pts) as f64 / f64::from(decoder.rate()))
        }
    }
}

#[cfg(feature = "pg_bench")]
#[pg_schema]
mod benches {
    use crate::bench_common::{generate_sample_video, sample_video_bytes};
    use pgrx::pg_bench;
    use pgrx_bench::{black_box, Bencher};

    #[pg_bench(setup = generate_sample_video)]
    fn bench_trim_fast(b: &mut Bencher) {
        let data = sample_video_bytes();
        b.iter(move || black_box(super::trim(data.clone(), 5.0, Some(15.0), false)));
    }

    #[pg_bench(setup = generate_sample_video)]
    fn bench_trim_precise(b: &mut Bencher) {
        let data = sample_video_bytes();
        b.iter(move || black_box(super::trim(data.clone(), 5.0, Some(15.0), true)));
    }
}
