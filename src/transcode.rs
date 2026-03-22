use pgrx::prelude::*;

use crate::mem_io::{MemInput, MemOutput};

#[pg_extern]
fn transcode(
    data: Vec<u8>,
    format: default!(Option<&str>, "NULL"),
    filter: default!(Option<&str>, "NULL"),
) -> Vec<u8> {
    use ffmpeg_next::codec;
    use ffmpeg_next::filter;
    use ffmpeg_next::media::Type;
    use ffmpeg_next::util::frame::video::Video;

    ffmpeg_next::init().unwrap();

    let filter_spec = filter.unwrap_or("null");

    let mut ictx = MemInput::open(data);
    let input_format = ictx.format().name().to_owned();
    let out_format = format.unwrap_or(&input_format);
    let mut octx = MemOutput::open(out_format);

    // Find best video stream and create decoder
    let video_stream = ictx
        .streams()
        .best(Type::Video)
        .unwrap_or_else(|| error!("no video stream found"));
    let video_stream_index = video_stream.index();
    let video_time_base = video_stream.time_base();
    let video_frame_rate = video_stream.rate();

    let decoder_ctx =
        codec::context::Context::from_parameters(video_stream.parameters())
            .unwrap_or_else(|e| error!("failed to create decoder context: {e}"));
    let mut decoder = decoder_ctx
        .decoder()
        .video()
        .unwrap_or_else(|e| error!("failed to open decoder: {e}"));

    // Build filter graph: buffer → user spec → buffersink
    let mut graph = filter::Graph::new();
    let pix_fmt: i32 = unsafe { std::mem::transmute::<ffmpeg_next::format::Pixel, i16>(decoder.format()) } as i32;
    let aspect = decoder.aspect_ratio();
    let buffer_args = format!(
        "video_size={}x{}:pix_fmt={}:time_base={}/{}:pixel_aspect={}/{}",
        decoder.width(),
        decoder.height(),
        pix_fmt,
        video_time_base.numerator(),
        video_time_base.denominator(),
        aspect.numerator().max(1),
        aspect.denominator().max(1),
    );
    graph
        .add(&filter::find("buffer").unwrap(), "in", &buffer_args)
        .unwrap_or_else(|e| error!("failed to add buffer source: {e}"));
    graph
        .add(&filter::find("buffersink").unwrap(), "out", "")
        .unwrap_or_else(|e| error!("failed to add buffer sink: {e}"));
    graph
        .output("in", 0)
        .unwrap()
        .input("out", 0)
        .unwrap()
        .parse(filter_spec)
        .unwrap_or_else(|e| error!("failed to parse filter '{}': {e}", filter_spec));
    graph
        .validate()
        .unwrap_or_else(|e| error!("failed to validate filter graph: {e}"));

    // Read resolved output dimensions/format from the validated buffersink
    let (out_width, out_height, out_pix_fmt, filter_tb) = unsafe {
        let sink_ptr = graph.get("out").unwrap().as_ptr();
        let w = ffmpeg_next::sys::av_buffersink_get_w(sink_ptr) as u32;
        let h = ffmpeg_next::sys::av_buffersink_get_h(sink_ptr) as u32;
        let fmt = ffmpeg_next::sys::av_buffersink_get_format(sink_ptr);
        let tb = ffmpeg_next::sys::av_buffersink_get_time_base(sink_ptr);
        (
            w,
            h,
            std::mem::transmute::<i16, ffmpeg_next::format::Pixel>(fmt as i16),
            ffmpeg_next::Rational(tb.num, tb.den),
        )
    };

    // Create encoder matching the filter output
    let codec_id = decoder.id();
    let enc_codec = codec::encoder::find(codec_id)
        .unwrap_or_else(|| error!("no encoder found for codec {:?}", codec_id));
    let enc_ctx = codec::context::Context::new_with_codec(enc_codec);
    let mut video_enc = enc_ctx
        .encoder()
        .video()
        .unwrap_or_else(|e| error!("failed to create video encoder: {e}"));
    video_enc.set_width(out_width);
    video_enc.set_height(out_height);
    video_enc.set_format(out_pix_fmt);
    // Derive time_base from the stream's frame rate. Container time_bases (e.g. 1/90000
    // from mpegts) are unsuitable — codecs like MPEG2VIDEO need a standard frame rate.
    // Note: decoder.frame_rate() is unreliable here because AVCodecContext::framerate
    // is not populated by from_parameters(); use the stream's r_frame_rate instead.
    let enc_time_base = if video_frame_rate.numerator() != 0 {
        video_enc.set_frame_rate(Some(video_frame_rate));
        ffmpeg_next::Rational(video_frame_rate.denominator(), video_frame_rate.numerator())
    } else if filter_tb.denominator() != 0 {
        filter_tb
    } else {
        video_time_base
    };
    video_enc.set_time_base(enc_time_base);
    video_enc.set_bit_rate(decoder.bit_rate());
    // Set encoder-specific fields not available in AVCodecParameters.
    // The decoder context created via from_parameters() has gop_size=0,
    // which is invalid for codecs like MPEG2VIDEO, so use sensible defaults.
    unsafe {
        let enc_ptr = video_enc.as_mut_ptr();
        (*enc_ptr).gop_size = 12;
        (*enc_ptr).max_b_frames = 2;
    }
    let mut encoder = video_enc
        .open_as(enc_codec)
        .unwrap_or_else(|e| error!("failed to open encoder: {e}"));

    // Add video output stream
    let mut video_out_stream = octx
        .add_stream(enc_codec)
        .unwrap_or_else(|e| error!("failed to add video output stream: {e}"));
    video_out_stream.set_parameters(&encoder);
    let video_out_idx = video_out_stream.index();

    // Add non-video streams as passthrough
    let mut stream_map = vec![None::<usize>; ictx.streams().count()];
    stream_map[video_stream_index] = Some(video_out_idx);
    let mut out_idx = video_out_idx + 1;
    for input_stream in ictx.streams() {
        let idx = input_stream.index();
        if idx == video_stream_index {
            continue;
        }
        let medium = input_stream.parameters().medium();
        if medium == Type::Audio || medium == Type::Subtitle {
            let mut ns = octx
                .add_stream(codec::Id::None)
                .unwrap_or_else(|e| error!("failed to add output stream: {e}"));
            ns.set_parameters(input_stream.parameters());
            stream_map[idx] = Some(out_idx);
            out_idx += 1;
        }
    }

    octx.write_header()
        .unwrap_or_else(|e| error!("failed to write header: {e}"));

    let enc_tb = encoder.time_base();

    // Helper: drain encoded packets from encoder and write to output
    let drain_encoder =
        |encoder: &mut codec::encoder::video::Encoder,
         octx: &mut ffmpeg_next::format::context::Output| {
            let mut encoded = ffmpeg_next::Packet::empty();
            while encoder.receive_packet(&mut encoded).is_ok() {
                encoded.set_stream(video_out_idx);
                encoded.rescale_ts(enc_tb, octx.stream(video_out_idx).unwrap().time_base());
                encoded.set_position(-1);
                encoded
                    .write_interleaved(octx)
                    .unwrap_or_else(|e| error!("failed to write video packet: {e}"));
            }
        };

    // Helper: pull filtered frames from graph, encode, and write
    let encode_filtered =
        |graph: &mut filter::Graph,
         encoder: &mut codec::encoder::video::Encoder,
         octx: &mut ffmpeg_next::format::context::Output| {
            let mut filtered = Video::empty();
            while graph
                .get("out")
                .unwrap()
                .sink()
                .frame(&mut filtered)
                .is_ok()
            {
                encoder
                    .send_frame(&filtered)
                    .unwrap_or_else(|e| error!("encode error: {e}"));
                drain_encoder(encoder, octx);
            }
        };

    // Main packet loop
    for (stream, mut packet) in ictx.packets() {
        let input_index = stream.index();

        if input_index == video_stream_index {
            // Decode → filter → encode
            packet.rescale_ts(stream.time_base(), decoder.time_base());
            decoder
                .send_packet(&packet)
                .unwrap_or_else(|e| error!("decode error: {e}"));

            let mut decoded = Video::empty();
            while decoder.receive_frame(&mut decoded).is_ok() {
                graph
                    .get("in")
                    .unwrap()
                    .source()
                    .add(&decoded)
                    .unwrap_or_else(|e| error!("filter source error: {e}"));
                encode_filtered(&mut graph, &mut encoder, &mut *octx);
            }
        } else if let Some(Some(oi)) = stream_map.get(input_index) {
            // Passthrough audio/subtitle
            let in_tb = stream.time_base();
            let out_tb = octx.stream(*oi).unwrap().time_base();
            packet.set_stream(*oi);
            packet.rescale_ts(in_tb, out_tb);
            packet.set_position(-1);
            packet
                .write_interleaved(&mut *octx)
                .unwrap_or_else(|e| error!("failed to write packet: {e}"));
        }
    }

    // Flush decoder
    let _ = decoder.send_eof();
    let mut decoded = Video::empty();
    while decoder.receive_frame(&mut decoded).is_ok() {
        let _ = graph.get("in").unwrap().source().add(&decoded);
        encode_filtered(&mut graph, &mut encoder, &mut *octx);
    }

    // Flush filter graph
    let _ = graph.get("in").unwrap().source().flush();
    encode_filtered(&mut graph, &mut encoder, &mut *octx);

    // Flush encoder
    let _ = encoder.send_eof();
    drain_encoder(&mut encoder, &mut *octx);

    octx.write_trailer()
        .unwrap_or_else(|e| error!("failed to write trailer: {e}"));

    octx.into_data()
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use super::*;

    /// Generate a minimal MPEG-TS video in memory and return the raw bytes.
    fn generate_test_video_bytes(width: u32, height: u32, fps: i32, duration_secs: i32) -> Vec<u8> {
        use ffmpeg_next::codec;
        use ffmpeg_next::format::Pixel;
        use ffmpeg_next::util::frame::video::Video;

        ffmpeg_next::init().unwrap();

        let total_frames = fps * duration_secs;
        let enc_codec = ffmpeg_next::encoder::find(codec::Id::MPEG2VIDEO)
            .expect("MPEG2VIDEO encoder not found");

        let mut octx = MemOutput::open("mpegts");

        let mut stream = octx.add_stream(enc_codec).expect("failed to add stream");
        stream.set_time_base((1, fps));

        let ctx = codec::context::Context::new_with_codec(enc_codec);
        let mut encoder = ctx.encoder().video().expect("failed to create encoder");
        encoder.set_width(width);
        encoder.set_height(height);
        encoder.set_format(Pixel::YUV420P);
        encoder.set_bit_rate(400_000);
        encoder.set_gop(10);
        encoder.set_max_b_frames(2);
        encoder.set_frame_rate(Some((fps, 1)));
        encoder.set_time_base((1, fps));

        let mut encoder = encoder.open().expect("failed to open encoder");
        stream.set_parameters(&encoder);
        let out_time_base = stream.time_base();
        drop(stream);

        octx.write_header().expect("failed to write header");

        let mut packet = ffmpeg_next::Packet::empty();
        for i in 0..total_frames {
            let mut frame = Video::new(Pixel::YUV420P, width, height);
            let y_data = frame.data_mut(0);
            for (j, byte) in y_data.iter_mut().enumerate() {
                *byte = ((i as usize * 3 + j) % 256) as u8;
            }
            for plane in 1..=2 {
                for byte in frame.data_mut(plane).iter_mut() {
                    *byte = 128;
                }
            }
            frame.set_pts(Some(i as i64));

            encoder.send_frame(&frame).expect("failed to send frame");
            while encoder.receive_packet(&mut packet).is_ok() {
                packet.set_stream(0);
                packet.rescale_ts((1, fps), out_time_base);
                packet
                    .write_interleaved(&mut *octx)
                    .expect("failed to write packet");
            }
        }

        encoder.send_eof().expect("failed to send eof");
        while encoder.receive_packet(&mut packet).is_ok() {
            packet.set_stream(0);
            packet.rescale_ts((1, fps), out_time_base);
            packet
                .write_interleaved(&mut *octx)
                .expect("failed to write packet");
        }

        octx.write_trailer().expect("failed to write trailer");
        octx.into_data()
    }

    #[pg_test]
    fn test_transcode_default_format() {
        let data = generate_test_video_bytes(64, 64, 10, 1);
        assert!(!data.is_empty());

        // Transcode with default format (should keep mpegts)
        let result = transcode(data.clone(), None, None);
        assert!(!result.is_empty());

        // Verify output is valid by probing it
        let probe = MemInput::open(result);
        let fmt = probe.format().name().to_owned();
        assert!(fmt.contains("mpegts"), "expected mpegts, got {fmt}");
    }

    #[pg_test]
    fn test_transcode_to_different_format() {
        let data = generate_test_video_bytes(64, 64, 10, 1);

        let result = transcode(data, Some("matroska"), None);
        assert!(!result.is_empty());

        let probe = MemInput::open(result);
        let fmt = probe.format().name().to_owned();
        assert!(
            fmt.contains("matroska"),
            "expected matroska, got {fmt}"
        );
    }

    #[pg_test]
    fn test_transcode_with_scale_filter() {
        let data = generate_test_video_bytes(64, 64, 10, 1);

        let result = transcode(data, None, Some("scale=32:32"));
        assert!(!result.is_empty());

        // Verify output dimensions changed
        let probe = MemInput::open(result);
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
