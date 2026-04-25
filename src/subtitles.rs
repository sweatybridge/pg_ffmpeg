use ffmpeg_next::codec;
use ffmpeg_next::codec::subtitle::Rect;
use ffmpeg_next::media::Type;
use ffmpeg_next::Rational;
use pgrx::prelude::*;

use crate::codec_lookup;
use crate::mem_io::MemInput;

#[pg_extern]
fn extract_subtitles(
    data: &[u8],
    format: default!(String, "'srt'"),
    stream_index: default!(Option<i32>, "NULL"),
) -> String {
    ffmpeg_next::init().unwrap();
    validate_subtitle_format(&format);

    let mut ictx = MemInput::open(data);
    let selected = select_subtitle_stream(&ictx, stream_index);
    ensure_supported_text_subtitle(selected.codec_id);

    let cues = collect_subtitle_cues(&mut ictx, selected);
    render_cues(&cues, &format)
}

#[derive(Clone, Copy)]
struct SelectedSubtitle {
    index: usize,
    time_base: Rational,
    codec_id: codec::Id,
}

struct Cue {
    start: f64,
    end: f64,
    text: String,
}

fn validate_subtitle_format(format: &str) {
    match format {
        "srt" | "ass" | "webvtt" => {}
        _ => error!("pg_ffmpeg: subtitle format must be srt, ass, or webvtt"),
    }
}

fn select_subtitle_stream(ictx: &MemInput<'_>, stream_index: Option<i32>) -> SelectedSubtitle {
    let stream = match stream_index {
        Some(index) if index < 0 => error!("pg_ffmpeg: subtitle stream_index must be >= 0"),
        Some(index) => ictx
            .streams()
            .find(|stream| stream.index() == index as usize)
            .unwrap_or_else(|| error!("pg_ffmpeg: subtitle stream_index {index} not found")),
        None => ictx
            .streams()
            .best(Type::Subtitle)
            .unwrap_or_else(|| error!("no subtitle stream in input")),
    };

    if stream.parameters().medium() != Type::Subtitle {
        error!(
            "pg_ffmpeg: stream_index {} is not a subtitle stream",
            stream.index()
        );
    }

    SelectedSubtitle {
        index: stream.index(),
        time_base: stream.time_base(),
        codec_id: stream.parameters().id(),
    }
}

fn ensure_supported_text_subtitle(codec_id: codec::Id) {
    match codec_id {
        codec::Id::SUBRIP
        | codec::Id::ASS
        | codec::Id::SSA
        | codec::Id::WEBVTT
        | codec::Id::MOV_TEXT => {}
        codec::Id::DVD_SUBTITLE | codec::Id::HDMV_PGS_SUBTITLE | codec::Id::DVB_SUBTITLE => {
            error!("image-based subtitles require OCR, use extract_frames + external OCR");
        }
        _ => error!(
            "pg_ffmpeg: unsupported subtitle codec '{}' for text extraction",
            codec_id.name()
        ),
    }
}

fn collect_subtitle_cues(ictx: &mut MemInput<'_>, selected: SelectedSubtitle) -> Vec<Cue> {
    let mut cues = Vec::new();
    let decoder_codec =
        codec_lookup::find_decoder(selected.codec_id).unwrap_or_else(|e| error!("{e}"));
    let stream = ictx
        .stream(selected.index)
        .unwrap_or_else(|| error!("pg_ffmpeg: subtitle stream disappeared"));
    let decoder_ctx = codec::context::Context::from_parameters(stream.parameters())
        .unwrap_or_else(|e| error!("failed to create subtitle decoder context: {e}"));
    let mut decoder = decoder_ctx
        .decoder()
        .open_as(decoder_codec)
        .and_then(|opened| opened.subtitle())
        .unwrap_or_else(|e| error!("failed to open subtitle decoder: {e}"));

    for (stream, packet) in ictx.packets() {
        if stream.index() != selected.index {
            continue;
        }

        let packet_start = packet
            .pts()
            .or_else(|| packet.dts())
            .map(|pts| timestamp_seconds(pts, selected.time_base))
            .unwrap_or(0.0);
        let packet_duration = timestamp_seconds(packet.duration(), selected.time_base);

        let mut subtitle = ffmpeg_next::Subtitle::new();
        let decoded = decoder
            .decode(&packet, &mut subtitle)
            .unwrap_or_else(|e| error!("subtitle decode error: {e}"));
        if decoded {
            append_decoded_subtitle_cues(
                &mut cues,
                &subtitle,
                selected.codec_id,
                packet_start,
                packet_duration,
            );
        }
        unsafe {
            ffmpeg_next::sys::avsubtitle_free(subtitle.as_mut_ptr());
        }
    }

    cues
}

fn append_decoded_subtitle_cues(
    cues: &mut Vec<Cue>,
    subtitle: &ffmpeg_next::Subtitle,
    codec_id: codec::Id,
    packet_start: f64,
    packet_duration: f64,
) {
    let base_start = subtitle
        .pts()
        .map(|pts| pts as f64 / f64::from(ffmpeg_next::ffi::AV_TIME_BASE))
        .unwrap_or(packet_start);
    let start = base_start + f64::from(subtitle.start()) / 1000.0;
    let end = if subtitle.end() > subtitle.start() {
        base_start + f64::from(subtitle.end()) / 1000.0
    } else if packet_duration > 0.0 {
        packet_start + packet_duration
    } else {
        start
    };

    for rect in subtitle.rects() {
        let text = match rect {
            Rect::Text(text) => normalize_subtitle_text(text.get()),
            Rect::Ass(ass) => clean_ass_text(ass.get()),
            Rect::Bitmap(_) => {
                error!("image-based subtitles require OCR, use extract_frames + external OCR");
            }
            Rect::None(_) => String::new(),
        };
        if text.trim().is_empty() {
            continue;
        }
        let text = match codec_id {
            codec::Id::ASS | codec::Id::SSA => clean_ass_text(&text),
            _ => text,
        };
        cues.push(Cue {
            start,
            end: end.max(start + 0.001),
            text,
        });
    }
}

fn normalize_subtitle_text(text: &str) -> String {
    text.trim_matches(char::from(0))
        .replace("\r\n", "\n")
        .replace('\r', "\n")
        .trim()
        .to_owned()
}

fn clean_ass_text(text: &str) -> String {
    let event = text
        .strip_prefix("Dialogue:")
        .map(str::trim_start)
        .unwrap_or(text);
    let fields = event.splitn(10, ',').collect::<Vec<_>>();
    let body = if fields.len() == 10 { fields[9] } else { event };
    strip_ass_override_tags(body)
        .replace("\\N", "\n")
        .replace("\\n", "\n")
        .trim()
        .to_owned()
}

fn strip_ass_override_tags(text: &str) -> String {
    let mut output = String::with_capacity(text.len());
    let mut in_tag = false;

    for ch in text.chars() {
        match ch {
            '{' => in_tag = true,
            '}' => in_tag = false,
            _ if !in_tag => output.push(ch),
            _ => {}
        }
    }

    output
}

fn render_cues(cues: &[Cue], format: &str) -> String {
    match format {
        "srt" => render_srt(cues),
        "webvtt" => render_webvtt(cues),
        "ass" => render_ass(cues),
        _ => unreachable!("format is validated before subtitle rendering"),
    }
}

fn render_srt(cues: &[Cue]) -> String {
    let mut out = String::new();
    for (idx, cue) in cues.iter().enumerate() {
        out.push_str(&(idx + 1).to_string());
        out.push('\n');
        out.push_str(&format_srt_time(cue.start));
        out.push_str(" --> ");
        out.push_str(&format_srt_time(cue.end));
        out.push('\n');
        out.push_str(cue.text.trim());
        out.push_str("\n\n");
    }
    out
}

fn render_webvtt(cues: &[Cue]) -> String {
    let mut out = String::from("WEBVTT\n\n");
    for cue in cues {
        out.push_str(&format_webvtt_time(cue.start));
        out.push_str(" --> ");
        out.push_str(&format_webvtt_time(cue.end));
        out.push('\n');
        out.push_str(cue.text.trim());
        out.push_str("\n\n");
    }
    out
}

fn render_ass(cues: &[Cue]) -> String {
    let mut out = String::from(
        "[Script Info]\nScriptType: v4.00+\n\n[V4+ Styles]\nFormat: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding\nStyle: Default,Arial,20,&H00FFFFFF,&H000000FF,&H00000000,&H00000000,0,0,0,0,100,100,0,0,1,2,2,2,10,10,10,1\n\n[Events]\nFormat: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text\n",
    );
    for cue in cues {
        out.push_str("Dialogue: 0,");
        out.push_str(&format_ass_time(cue.start));
        out.push(',');
        out.push_str(&format_ass_time(cue.end));
        out.push_str(",Default,,0,0,0,,");
        out.push_str(&cue.text.trim().replace('\n', "\\N"));
        out.push('\n');
    }
    out
}

fn timestamp_seconds(timestamp: i64, time_base: Rational) -> f64 {
    if time_base.denominator() == 0 {
        return 0.0;
    }
    f64::from(time_base.numerator()) * timestamp as f64 / f64::from(time_base.denominator())
}

fn format_srt_time(seconds: f64) -> String {
    format_timestamp(seconds, ',')
}

fn format_webvtt_time(seconds: f64) -> String {
    format_timestamp(seconds, '.')
}

fn format_timestamp(seconds: f64, decimal_separator: char) -> String {
    let millis = (seconds.max(0.0) * 1000.0).round() as u64;
    let hours = millis / 3_600_000;
    let minutes = (millis / 60_000) % 60;
    let secs = (millis / 1000) % 60;
    let ms = millis % 1000;
    format!("{hours:02}:{minutes:02}:{secs:02}{decimal_separator}{ms:03}")
}

fn format_ass_time(seconds: f64) -> String {
    let centis = (seconds.max(0.0) * 100.0).round() as u64;
    let hours = centis / 360_000;
    let minutes = (centis / 6_000) % 60;
    let secs = (centis / 100) % 60;
    let cs = centis % 100;
    format!("{hours}:{minutes:02}:{secs:02}.{cs:02}")
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use super::*;
    use crate::test_utils::generate_test_video_bytes;

    #[pg_test]
    fn test_extract_subtitles_srt_to_webvtt() {
        let data = b"1\n00:00:00,000 --> 00:00:01,250\nHello from pg_ffmpeg\n\n".to_vec();
        let webvtt = extract_subtitles(&data, "webvtt".to_string(), None);

        assert!(webvtt.starts_with("WEBVTT\n\n"));
        assert!(webvtt.contains("00:00:00.000 --> 00:00:01.250"));
        assert!(webvtt.contains("Hello from pg_ffmpeg"));
    }

    #[pg_test]
    #[should_panic(expected = "image-based subtitles require OCR")]
    fn test_extract_subtitles_rejects_pgs() {
        ensure_supported_text_subtitle(codec::Id::HDMV_PGS_SUBTITLE);
    }

    #[pg_test]
    #[should_panic(expected = "no subtitle stream in input")]
    fn test_extract_subtitles_no_stream_errors() {
        let data = generate_test_video_bytes(64, 64, 10, 1);
        extract_subtitles(&data, "srt".to_string(), None);
    }
}
