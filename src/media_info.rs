use pgrx::prelude::*;
use serde_json::json;

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

        let mut info = json!({
            "index": stream.index(),
            "type": format!("{:?}", medium).to_lowercase(),
            "codec": format!("{:?}", codec_id).to_lowercase(),
        });

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

    let result = json!({
        "format": ictx.format().name(),
        "duration": duration_secs,
        "bit_rate": ictx.bit_rate(),
        "streams": streams,
    });

    pgrx::JsonB(result)
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
