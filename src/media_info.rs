use pgrx::prelude::*;
use serde_json::{json, Map, Value};

use crate::mem_io::MemInput;

#[pg_extern]
fn media_info(data: Vec<u8>) -> pgrx::JsonB {
    ffmpeg_next::init().unwrap();

    let ictx = MemInput::open(&data);

    let duration_secs = if ictx.duration() >= 0 {
        Some(ictx.duration() as f64 / f64::from(ffmpeg_next::ffi::AV_TIME_BASE))
    } else {
        None
    };

    let mut streams = Vec::new();
    for stream in ictx.streams() {
        let medium = stream.parameters().medium();
        let codec_id = stream.parameters().id();
        let tags = dictionary_to_json(stream.metadata());
        let language = stream.metadata().get("language").map(str::to_owned);

        let mut info = json!({
            "index": stream.index(),
            "type": format!("{:?}", medium).to_lowercase(),
            "codec": format!("{:?}", codec_id).to_lowercase(),
            "bit_rate": stream_bit_rate(&stream),
            "disposition": disposition_names(stream.disposition()),
            "tags": tags,
            "language": language,
        });

        if medium == ffmpeg_next::media::Type::Subtitle {
            info["codec_type"] = json!(subtitle_codec_type(codec_id));
        }

        if medium == ffmpeg_next::media::Type::Video {
            if let Ok(ctx) =
                ffmpeg_next::codec::context::Context::from_parameters(stream.parameters())
            {
                if let Ok(video) = ctx.decoder().video() {
                    info["width"] = json!(video.width());
                    info["height"] = json!(video.height());
                    let rate = stream.avg_frame_rate();
                    if rate.denominator() != 0 {
                        info["fps"] =
                            json!(f64::from(rate.numerator()) / f64::from(rate.denominator()));
                    }
                }
            }
        } else if medium == ffmpeg_next::media::Type::Audio {
            if let Ok(ctx) =
                ffmpeg_next::codec::context::Context::from_parameters(stream.parameters())
            {
                if let Ok(audio) = ctx.decoder().audio() {
                    info["sample_rate"] = json!(audio.rate());
                    info["channels"] = json!(audio.channels());
                }
            }
        }

        streams.push(info);
    }

    let chapters = ictx
        .chapters()
        .map(|chapter| {
            let time_base = chapter.time_base();
            json!({
                "id": chapter.id(),
                "start": timestamp_seconds(chapter.start(), time_base),
                "end": timestamp_seconds(chapter.end(), time_base),
                "title": chapter.metadata().get("title"),
            })
        })
        .collect::<Vec<_>>();

    let result = json!({
        "format": ictx.format().name(),
        "duration": duration_secs,
        "bit_rate": ictx.bit_rate(),
        "chapters": chapters,
        "tags": dictionary_to_json(ictx.metadata()),
        "streams": streams,
    });

    pgrx::JsonB(result)
}

fn dictionary_to_json(dictionary: ffmpeg_next::DictionaryRef<'_>) -> Map<String, Value> {
    dictionary
        .iter()
        .map(|(key, value)| (key.to_owned(), json!(value)))
        .collect()
}

fn disposition_names(disposition: ffmpeg_next::format::stream::Disposition) -> Vec<&'static str> {
    let mut names = Vec::new();

    if disposition.contains(ffmpeg_next::format::stream::Disposition::DEFAULT) {
        names.push("default");
    }
    if disposition.contains(ffmpeg_next::format::stream::Disposition::DUB) {
        names.push("dub");
    }
    if disposition.contains(ffmpeg_next::format::stream::Disposition::ORIGINAL) {
        names.push("original");
    }
    if disposition.contains(ffmpeg_next::format::stream::Disposition::COMMENT) {
        names.push("comment");
    }
    if disposition.contains(ffmpeg_next::format::stream::Disposition::LYRICS) {
        names.push("lyrics");
    }
    if disposition.contains(ffmpeg_next::format::stream::Disposition::KARAOKE) {
        names.push("karaoke");
    }
    if disposition.contains(ffmpeg_next::format::stream::Disposition::FORCED) {
        names.push("forced");
    }
    if disposition.contains(ffmpeg_next::format::stream::Disposition::HEARING_IMPAIRED) {
        names.push("hearing_impaired");
    }
    if disposition.contains(ffmpeg_next::format::stream::Disposition::VISUAL_IMPAIRED) {
        names.push("visual_impaired");
    }
    if disposition.contains(ffmpeg_next::format::stream::Disposition::CLEAN_EFFECTS) {
        names.push("clean_effects");
    }
    if disposition.contains(ffmpeg_next::format::stream::Disposition::ATTACHED_PIC) {
        names.push("attached_pic");
    }
    if disposition.contains(ffmpeg_next::format::stream::Disposition::CAPTIONS) {
        names.push("captions");
    }
    if disposition.contains(ffmpeg_next::format::stream::Disposition::DESCRIPTIONS) {
        names.push("descriptions");
    }
    if disposition.contains(ffmpeg_next::format::stream::Disposition::METADATA) {
        names.push("metadata");
    }
    names
}

fn stream_bit_rate(stream: &ffmpeg_next::format::stream::Stream<'_>) -> Option<i64> {
    let parameters = stream.parameters();
    let params_bit_rate = unsafe { (*parameters.as_ptr()).bit_rate };
    if params_bit_rate > 0 {
        return Some(params_bit_rate);
    }

    let context =
        ffmpeg_next::codec::context::Context::from_parameters(stream.parameters()).ok()?;
    let context_bit_rate = unsafe { (*context.as_ptr()).bit_rate };
    (context_bit_rate > 0).then_some(context_bit_rate)
}

fn subtitle_codec_type(codec_id: ffmpeg_next::codec::Id) -> Option<&'static str> {
    unsafe {
        let descriptor = ffmpeg_next::ffi::avcodec_descriptor_get(codec_id.into());
        if descriptor.is_null() {
            return None;
        }

        let props = (*descriptor).props;
        if props & ffmpeg_next::ffi::AV_CODEC_PROP_TEXT_SUB != 0 {
            Some("text")
        } else if props & ffmpeg_next::ffi::AV_CODEC_PROP_BITMAP_SUB != 0 {
            Some("image")
        } else {
            None
        }
    }
}

fn timestamp_seconds(timestamp: i64, time_base: ffmpeg_next::Rational) -> Option<f64> {
    if time_base.denominator() == 0 {
        None
    } else {
        Some(timestamp as f64 * f64::from(time_base))
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use super::*;
    use crate::mem_io::{MemInput, MemOutput};
    use crate::{pipeline, test_utils::generate_test_video_bytes};

    use ffmpeg_next::{Dictionary, Rational};

    fn generate_tagged_chaptered_video_bytes() -> Vec<u8> {
        ffmpeg_next::init().unwrap();

        let input_bytes = generate_test_video_bytes(64, 64, 25, 2);
        let mut ictx = MemInput::open(&input_bytes);
        let mut octx = MemOutput::open("matroska");

        let input_stream = ictx
            .streams()
            .best(ffmpeg_next::media::Type::Video)
            .expect("expected a video stream");
        let input_stream_index = input_stream.index();
        let output_stream_index = pipeline::copy_stream(&input_stream, &mut octx);

        {
            let mut output_stream = octx
                .stream_mut(output_stream_index)
                .expect("expected copied output stream");
            output_stream.set_metadata(
                [
                    ("language", "eng"),
                    ("title", "Primary video"),
                    ("handler_name", "pg_ffmpeg test stream"),
                ]
                .into_iter()
                .collect::<Dictionary<'static>>(),
            );
        }

        octx.set_metadata(
            [("title", "Tagged sample"), ("genre", "test-fixture")]
                .into_iter()
                .collect::<Dictionary<'static>>(),
        );
        octx.add_chapter(1, Rational::new(1, 1000), 0, 1000, "Intro")
            .expect("failed to add intro chapter");
        octx.add_chapter(2, Rational::new(1, 1000), 1000, 2000, "Main")
            .expect("failed to add main chapter");

        octx.write_header()
            .expect("failed to write metadata fixture header");

        let output_time_base = octx
            .stream(output_stream_index)
            .expect("expected output stream")
            .time_base();

        for (stream, mut packet) in ictx.packets() {
            if stream.index() != input_stream_index {
                continue;
            }

            packet.set_stream(output_stream_index);
            packet.rescale_ts(stream.time_base(), output_time_base);
            packet.set_position(-1);
            packet
                .write_interleaved(&mut octx)
                .expect("failed to write metadata fixture packet");
        }

        octx.write_trailer()
            .expect("failed to write metadata fixture trailer");
        octx.into_data()
    }

    #[pg_test]
    fn test_media_info_tags() {
        let data = generate_tagged_chaptered_video_bytes();
        let pgrx::JsonB(info) = media_info(data);

        assert_eq!(info["tags"]["title"], "Tagged sample");
        assert!(
            info["tags"]
                .as_object()
                .is_some_and(|tags| !tags.is_empty()),
            "expected at least one format-level tag"
        );

        let stream = &info["streams"][0];
        assert_eq!(stream["language"], "eng");
        assert_eq!(stream["tags"]["language"], "eng");
        assert_eq!(stream["tags"]["title"], "Primary video");
        assert!(
            stream
                .as_object()
                .is_some_and(|stream| stream.contains_key("bit_rate")),
            "expected stream bit_rate key to be present"
        );
        assert!(
            stream["disposition"].is_array(),
            "expected disposition to be returned as an array of enabled flags"
        );
    }

    #[pg_test]
    fn test_media_info_chapters_present() {
        let data = generate_tagged_chaptered_video_bytes();
        let pgrx::JsonB(info) = media_info(data);

        let chapters = info["chapters"].as_array().expect("expected chapter array");
        assert_eq!(chapters.len(), 2);
        assert_eq!(chapters[0]["id"], 1);
        assert_eq!(chapters[0]["title"], "Intro");
        assert_eq!(chapters[1]["id"], 2);
        assert_eq!(chapters[1]["title"], "Main");

        let first_start = chapters[0]["start"]
            .as_f64()
            .expect("expected numeric chapter start");
        let first_end = chapters[0]["end"]
            .as_f64()
            .expect("expected numeric chapter end");
        let second_start = chapters[1]["start"]
            .as_f64()
            .expect("expected numeric chapter start");

        assert!(
            first_start.abs() < 1e-9,
            "expected first chapter to start at 0"
        );
        assert!(
            (first_end - 1.0).abs() < 1e-6,
            "expected first chapter to end at 1 second"
        );
        assert!(
            (second_start - 1.0).abs() < 1e-6,
            "expected second chapter to start at 1 second"
        );
    }
}

#[cfg(feature = "pg_bench")]
#[pg_schema]
mod benches {
    use crate::bench_common::{generate_sample_video, sample_video_bytes};
    use pgrx::pg_bench;
    use pgrx_bench::{black_box, Bencher};

    #[pg_bench(setup = generate_sample_video)]
    fn bench_media_info(b: &mut Bencher) {
        let data = sample_video_bytes();
        b.iter(move || black_box(super::media_info(data.clone())));
    }
}
