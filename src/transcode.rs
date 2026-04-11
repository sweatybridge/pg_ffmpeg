use pgrx::prelude::*;

use crate::mem_io::{MemInput, MemOutput};
use crate::pipeline::{self, VideoPipeline};

use ffmpeg_next::codec;
use ffmpeg_next::media::Type;
use ffmpeg_next::Rational;

use std::collections::HashMap;

#[pg_extern]
fn transcode(
    data: Vec<u8>,
    format: default!(Option<&str>, "NULL"),
    filter: default!(Option<&str>, "NULL"),
) -> Vec<u8> {
    ffmpeg_next::init().unwrap();

    let mut ictx = MemInput::open(&data);
    let input_format = ictx.format().name().to_owned();
    // Map input-only formats to their output equivalents
    let default_format = match input_format.as_str() {
        "png_pipe" | "ppm_pipe" => "image2pipe".to_owned(),
        _ => input_format.clone(),
    };
    let out_format = format.unwrap_or(&default_format);

    // Zero-copy path: no filter requested → remux packets without decode/encode
    if filter.is_none() {
        let mut octx = MemOutput::open(out_format);

        let mut stream_map = vec![None::<usize>; ictx.streams().count()];
        for input_stream in ictx.streams() {
            let out_idx = pipeline::copy_stream(&input_stream, &mut octx);
            stream_map[input_stream.index()] = Some(out_idx);
        }

        octx.write_header()
            .unwrap_or_else(|e| error!("failed to write header: {e}"));

        for (stream, mut packet) in ictx.packets() {
            if let Some(Some(oi)) = stream_map.get(stream.index()) {
                let in_tb = stream.time_base();
                let out_tb = octx.stream(*oi).unwrap().time_base();
                packet.set_stream(*oi);
                packet.rescale_ts(in_tb, out_tb);
                packet.set_position(-1);
                packet
                    .write_interleaved(&mut octx)
                    .unwrap_or_else(|e| error!("failed to write packet: {e}"));
            }
        }

        octx.write_trailer()
            .unwrap_or_else(|e| error!("failed to write trailer: {e}"));

        return octx.into_data();
    }

    let filter_spec = filter.unwrap_or("null");
    let mut octx = MemOutput::open(out_format);

    // Identify video streams and build pipelines
    let mut stream_mapping: Vec<isize> = vec![-1; ictx.streams().count()];
    let mut ist_time_bases = vec![Rational(0, 0); ictx.streams().count()];
    let mut pipelines: HashMap<usize, VideoPipeline> = HashMap::new();
    let mut ost_index: usize = 0;

    for (ist_index, ist) in ictx.streams().enumerate() {
        let ist_medium = ist.parameters().medium();
        if ist_medium != Type::Audio && ist_medium != Type::Video && ist_medium != Type::Subtitle {
            continue;
        }
        ist_time_bases[ist_index] = ist.time_base();

        if ist_medium == Type::Video {
            pipelines.insert(
                ist_index,
                VideoPipeline::new(&ist, &mut octx, ost_index, filter_spec),
            );
            stream_mapping[ist_index] = ost_index as isize;
        } else {
            // Set up stream copy for non-video streams
            let mut ost = octx
                .add_stream(codec::Id::None)
                .unwrap_or_else(|e| error!("failed to add output stream: {e}"));
            ost.set_parameters(ist.parameters());
            unsafe { (*ost.parameters().as_mut_ptr()).codec_tag = 0 };
            stream_mapping[ist_index] = ost_index as isize;
        }
        ost_index += 1;
    }

    octx.write_header()
        .unwrap_or_else(|e| error!("failed to write header: {e}"));

    // Collect output time bases after header is written
    let mut ost_time_bases = vec![Rational(0, 0); octx.streams().count()];
    for (i, _) in octx.streams().enumerate() {
        ost_time_bases[i] = octx.stream(i as _).unwrap().time_base();
    }

    // Main packet loop
    for (stream, mut packet) in ictx.packets() {
        let ist_index = stream.index();
        let ost_idx = stream_mapping[ist_index];
        if ost_idx < 0 {
            continue;
        }
        let ost_time_base = ost_time_bases[ost_idx as usize];

        match pipelines.get_mut(&ist_index) {
            Some(pipe) => {
                packet.rescale_ts(stream.time_base(), pipe.decoder_time_base());
                pipe.send_packet_to_decoder(&packet);
                pipe.receive_and_process_decoded_frames(&mut octx, ost_time_base);
            }
            None => {
                // Stream copy for non-video streams
                packet.rescale_ts(ist_time_bases[ist_index], ost_time_base);
                packet.set_position(-1);
                packet.set_stream(ost_idx as _);
                packet
                    .write_interleaved(&mut octx)
                    .unwrap_or_else(|e| error!("failed to write packet: {e}"));
            }
        }
    }

    // Flush decoders, filters, and encoders
    for (ist_index, pipe) in pipelines.iter_mut() {
        let ost_time_base = ost_time_bases[stream_mapping[*ist_index] as usize];
        pipe.send_eof_to_decoder();
        pipe.receive_and_process_decoded_frames(&mut octx, ost_time_base);
        pipe.flush_filter();
        pipe.receive_and_process_filtered_frames(&mut octx, ost_time_base);
        pipe.send_eof_to_encoder();
        pipe.receive_and_process_encoded_packets(&mut octx, ost_time_base);
    }

    octx.write_trailer()
        .unwrap_or_else(|e| error!("failed to write trailer: {e}"));

    octx.into_data()
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use super::*;
    use crate::test_utils::generate_test_video_bytes;

    #[pg_test]
    fn test_transcode_default_format() {
        let data = generate_test_video_bytes(64, 64, 10, 1);
        assert!(!data.is_empty());

        // Transcode with default format (should keep mpegts)
        let result = transcode(data.clone(), None, None);
        assert!(!result.is_empty());

        // Verify output is valid by probing it
        let probe = MemInput::open(&result);
        let fmt = probe.format().name().to_owned();
        assert!(fmt.contains("mpegts"), "expected mpegts, got {fmt}");
    }

    #[pg_test]
    fn test_transcode_to_different_format() {
        let data = generate_test_video_bytes(64, 64, 10, 1);

        let result = transcode(data, Some("matroska"), None);
        assert!(!result.is_empty());

        let probe = MemInput::open(&result);
        let fmt = probe.format().name().to_owned();
        assert!(fmt.contains("matroska"), "expected matroska, got {fmt}");
    }

    #[pg_test]
    fn test_transcode_image() {
        use ffmpeg_next::codec;
        use ffmpeg_next::format::Pixel;
        use ffmpeg_next::util::frame::video::Video;

        ffmpeg_next::init().unwrap();

        // Generate a single-frame PNG in memory
        let enc_codec = ffmpeg_next::encoder::find(codec::Id::PNG).expect("PNG encoder not found");
        let mut octx = MemOutput::open("image2pipe");
        let mut stream = octx.add_stream(enc_codec).expect("failed to add stream");
        stream.set_time_base((1, 1));

        let ctx = codec::context::Context::new_with_codec(enc_codec);
        let mut encoder = ctx.encoder().video().expect("failed to create encoder");
        encoder.set_width(64);
        encoder.set_height(64);
        encoder.set_format(Pixel::RGB24);
        encoder.set_time_base((1, 1));

        let mut encoder = encoder.open().expect("failed to open encoder");
        stream.set_parameters(&encoder);
        let out_time_base = stream.time_base();
        drop(stream);

        octx.write_header().expect("failed to write header");

        let mut frame = Video::new(Pixel::RGB24, 64, 64);
        for (i, byte) in frame.data_mut(0).iter_mut().enumerate() {
            *byte = (i % 256) as u8;
        }
        frame.set_pts(Some(0));

        encoder.send_frame(&frame).expect("failed to send frame");
        encoder.send_eof().expect("failed to send eof");
        let mut packet = ffmpeg_next::Packet::empty();
        while encoder.receive_packet(&mut packet).is_ok() {
            packet.set_stream(0);
            packet.rescale_ts((1, 1), out_time_base);
            packet
                .write_interleaved(&mut *octx)
                .expect("failed to write packet");
        }

        octx.write_trailer().expect("failed to write trailer");
        let ppm_data = octx.into_data();
        assert!(!ppm_data.is_empty());

        // Transcode the PPM image with a crop filter
        let result = transcode(ppm_data, None, Some("crop=32:32:0:0"));
        assert!(!result.is_empty());

        // Verify output dimensions match the crop
        let probe = MemInput::open(&result);
        let video = probe
            .streams()
            .best(ffmpeg_next::media::Type::Video)
            .expect("no video stream in output");
        let params = video.parameters();
        let ctx = ffmpeg_next::codec::context::Context::from_parameters(params).unwrap();
        let dec = ctx.decoder().video().unwrap();
        assert_eq!(dec.width(), 32);
        assert_eq!(dec.height(), 32);
    }

    #[pg_test]
    fn test_transcode_with_scale_filter() {
        let data = generate_test_video_bytes(64, 64, 10, 1);

        let result = transcode(data, None, Some("scale=32:32"));
        assert!(!result.is_empty());

        // Verify output dimensions changed
        let probe = MemInput::open(&result);
        let video = probe
            .streams()
            .best(ffmpeg_next::media::Type::Video)
            .expect("no video stream in output");
        let params = video.parameters();
        let ctx = ffmpeg_next::codec::context::Context::from_parameters(params).unwrap();
        let dec = ctx.decoder().video().unwrap();
        assert_eq!(dec.width(), 32);
        assert_eq!(dec.height(), 32);
    }
}

#[cfg(feature = "pg_bench")]
#[pg_schema]
mod benches {
    use crate::bench_common::{generate_sample_video, sample_video_bytes};
    use pgrx::pg_bench;
    use pgrx_bench::{black_box, Bencher};

    #[pg_bench(setup = generate_sample_video)]
    fn bench_transcode_remux(b: &mut Bencher) {
        let data = sample_video_bytes();
        b.iter(move || black_box(super::transcode(data.clone(), Some("matroska"), None)));
    }

    #[pg_bench(setup = generate_sample_video)]
    fn bench_transcode_filter_scale(b: &mut Bencher) {
        let data = sample_video_bytes();
        b.iter(move || black_box(super::transcode(data.clone(), None, Some("scale=320:240"))));
    }
}
