use ffmpeg_next::codec;
use ffmpeg_next::media::Type;
use ffmpeg_next::Rational;
use pgrx::prelude::*;

use crate::mem_io::MemInput;

#[pg_extern]
fn extract_subtitles(
    data: Vec<u8>,
    format: default!(String, "'srt'"),
    stream_index: default!(Option<i32>, "NULL"),
) -> String {
    ffmpeg_next::init().unwrap();
    validate_subtitle_format(&format);

    let mut ictx = MemInput::open(&data);
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
    let mut fallback_start = 0.0f64;

    for (stream, packet) in ictx.packets() {
        if stream.index() != selected.index {
            continue;
        }

        let Some(data) = packet.data() else {
            continue;
        };
        let text = packet_text(selected.codec_id, data);
        if text.trim().is_empty() {
            continue;
        }

        let start = packet
            .pts()
            .or_else(|| packet.dts())
            .map(|pts| timestamp_seconds(pts, selected.time_base))
            .unwrap_or(fallback_start);
        let duration = timestamp_seconds(packet.duration(), selected.time_base);
        let end = if duration > 0.0 {
            start + duration
        } else {
            start + 2.0
        };
        fallback_start = end;

        cues.push(Cue {
            start,
            end: end.max(start + 0.001),
            text,
        });
    }

    cues
}

fn packet_text(codec_id: codec::Id, data: &[u8]) -> String {
    let payload = match codec_id {
        codec::Id::MOV_TEXT if data.len() >= 2 => {
            let len = u16::from_be_bytes([data[0], data[1]]) as usize;
            if len <= data.len().saturating_sub(2) {
                &data[2..2 + len]
            } else {
                &data[2..]
            }
        }
        _ => data,
    };

    let text = String::from_utf8_lossy(payload)
        .trim_matches(char::from(0))
        .replace("\r\n", "\n")
        .replace('\r', "\n");

    match codec_id {
        codec::Id::ASS | codec::Id::SSA => clean_ass_text(&text),
        _ => text.trim().to_owned(),
    }
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
        let webvtt = extract_subtitles(data, "webvtt".to_string(), None);

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
        extract_subtitles(data, "srt".to_string(), None);
    }
}
