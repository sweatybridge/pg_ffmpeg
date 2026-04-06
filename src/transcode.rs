use pgrx::prelude::*;

use crate::mem_io::{MemInput, MemOutput};

use ffmpeg_next::codec;
use ffmpeg_next::filter;
use ffmpeg_next::media::Type;
use ffmpeg_next::util::frame::video::Video;
use ffmpeg_next::{frame, picture, Packet, Rational};

use std::collections::HashMap;

struct Transcoder {
    ost_index: usize,
    decoder: ffmpeg_next::decoder::Video,
    input_time_base: Rational,
    encoder: ffmpeg_next::encoder::Video,
    graph: filter::Graph,
}

impl Transcoder {
    fn new(
        ist: &ffmpeg_next::format::stream::Stream,
        octx: &mut ffmpeg_next::format::context::Output,
        ost_index: usize,
        filter_spec: &str,
    ) -> Self {
        let decoder_ctx = codec::context::Context::from_parameters(ist.parameters())
            .unwrap_or_else(|e| error!("failed to create decoder context: {e}"));
        let mut decoder = decoder_ctx
            .decoder()
            .video()
            .unwrap_or_else(|e| error!("failed to open decoder: {e}"));
        let input_time_base = ist.time_base();
        decoder.set_time_base(input_time_base);

        // Build filter graph: buffer → user spec → buffersink
        let mut graph = filter::Graph::new();
        let pix_fmt = Into::<ffmpeg_next::sys::AVPixelFormat>::into(decoder.format()) as i32;
        let aspect = decoder.aspect_ratio();
        let buffer_args = format!(
            "video_size={}x{}:pix_fmt={}:time_base={}/{}:pixel_aspect={}/{}",
            decoder.width(),
            decoder.height(),
            pix_fmt,
            input_time_base.numerator(),
            input_time_base.denominator(),
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
                ffmpeg_next::format::Pixel::from(std::mem::transmute::<
                    i32,
                    ffmpeg_next::sys::AVPixelFormat,
                >(fmt)),
                Rational(tb.num, tb.den),
            )
        };

        // Create encoder matching the input codec
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

        let enc_time_base = if let Some(frame_rate) = decoder.frame_rate() {
            video_enc.set_frame_rate(Some(frame_rate));
            Rational(frame_rate.denominator(), frame_rate.numerator())
        } else if filter_tb.denominator() != 0 {
            filter_tb
        } else {
            input_time_base
        };
        video_enc.set_time_base(enc_time_base);
        video_enc.set_bit_rate(decoder.bit_rate());

        unsafe {
            let dec_ptr = decoder.as_ptr();
            let enc_ptr = video_enc.as_mut_ptr();
            (*enc_ptr).gop_size = (*dec_ptr).gop_size;
            (*enc_ptr).max_b_frames = (*dec_ptr).max_b_frames;
        }

        let global_header = octx
            .format()
            .flags()
            .contains(ffmpeg_next::format::Flags::GLOBAL_HEADER);
        if global_header {
            video_enc.set_flags(codec::Flags::GLOBAL_HEADER);
        }

        let encoder = video_enc
            .open_as(enc_codec)
            .unwrap_or_else(|e| error!("failed to open encoder: {e}"));

        let mut ost = octx
            .add_stream(enc_codec)
            .unwrap_or_else(|e| error!("failed to add video output stream: {e}"));
        ost.set_parameters(&encoder);

        Self {
            ost_index,
            decoder,
            input_time_base,
            encoder,
            graph,
        }
    }

    fn send_packet_to_decoder(&mut self, packet: &Packet) {
        self.decoder
            .send_packet(packet)
            .unwrap_or_else(|e| error!("decode error: {e}"));
    }

    fn send_eof_to_decoder(&mut self) {
        let _ = self.decoder.send_eof();
    }

    fn receive_and_process_decoded_frames(
        &mut self,
        octx: &mut ffmpeg_next::format::context::Output,
        ost_time_base: Rational,
    ) {
        let mut decoded = Video::empty();
        while self.decoder.receive_frame(&mut decoded).is_ok() {
            let timestamp = decoded.timestamp();
            decoded.set_pts(timestamp);
            decoded.set_kind(picture::Type::None);
            self.graph
                .get("in")
                .unwrap()
                .source()
                .add(&decoded)
                .unwrap_or_else(|e| error!("filter source error: {e}"));
            self.receive_and_process_filtered_frames(octx, ost_time_base);
        }
    }

    fn receive_and_process_filtered_frames(
        &mut self,
        octx: &mut ffmpeg_next::format::context::Output,
        ost_time_base: Rational,
    ) {
        let mut filtered = frame::Video::empty();
        while self
            .graph
            .get("out")
            .unwrap()
            .sink()
            .frame(&mut filtered)
            .is_ok()
        {
            self.encoder
                .send_frame(&filtered)
                .unwrap_or_else(|e| error!("encode error: {e}"));
            self.receive_and_process_encoded_packets(octx, ost_time_base);
        }
    }

    fn flush_filter(&mut self) {
        let _ = self.graph.get("in").unwrap().source().flush();
    }

    fn send_eof_to_encoder(&mut self) {
        let _ = self.encoder.send_eof();
    }

    fn receive_and_process_encoded_packets(
        &mut self,
        octx: &mut ffmpeg_next::format::context::Output,
        ost_time_base: Rational,
    ) {
        let mut encoded = Packet::empty();
        while self.encoder.receive_packet(&mut encoded).is_ok() {
            encoded.set_stream(self.ost_index);
            encoded.rescale_ts(self.input_time_base, ost_time_base);
            encoded.set_position(-1);
            encoded
                .write_interleaved(octx)
                .unwrap_or_else(|e| error!("failed to write video packet: {e}"));
        }
    }
}

#[pg_extern]
fn transcode(
    data: Vec<u8>,
    format: default!(Option<&str>, "NULL"),
    filter: default!(Option<&str>, "NULL"),
) -> Vec<u8> {
    ffmpeg_next::init().unwrap();

    let mut ictx = MemInput::open(data);
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
            let mut out_stream = octx
                .add_stream(codec::Id::None)
                .unwrap_or_else(|e| error!("failed to add output stream: {e}"));
            out_stream.set_parameters(input_stream.parameters());
            unsafe { (*out_stream.parameters().as_mut_ptr()).codec_tag = 0 };
            stream_map[input_stream.index()] = Some(out_stream.index());
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

    // Identify video streams and build transcoders
    let _best_video_stream_index = ictx
        .streams()
        .best(Type::Video)
        .map(|stream| stream.index());
    let mut stream_mapping: Vec<isize> = vec![-1; ictx.streams().count()];
    let mut ist_time_bases = vec![Rational(0, 0); ictx.streams().count()];
    let mut transcoders = HashMap::new();
    let mut ost_index: usize = 0;

    for (ist_index, ist) in ictx.streams().enumerate() {
        let ist_medium = ist.parameters().medium();
        if ist_medium != Type::Audio && ist_medium != Type::Video && ist_medium != Type::Subtitle {
            continue;
        }
        ist_time_bases[ist_index] = ist.time_base();

        if ist_medium == Type::Video {
            transcoders.insert(
                ist_index,
                Transcoder::new(&ist, &mut octx, ost_index, filter_spec),
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

        match transcoders.get_mut(&ist_index) {
            Some(transcoder) => {
                packet.rescale_ts(stream.time_base(), transcoder.decoder.time_base());
                transcoder.send_packet_to_decoder(&packet);
                transcoder.receive_and_process_decoded_frames(&mut octx, ost_time_base);
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
    for (ist_index, transcoder) in transcoders.iter_mut() {
        let ost_time_base = ost_time_bases[stream_mapping[*ist_index] as usize];
        transcoder.send_eof_to_decoder();
        transcoder.receive_and_process_decoded_frames(&mut octx, ost_time_base);
        transcoder.flush_filter();
        transcoder.receive_and_process_filtered_frames(&mut octx, ost_time_base);
        transcoder.send_eof_to_encoder();
        transcoder.receive_and_process_encoded_packets(&mut octx, ost_time_base);
    }

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

#[cfg(feature = "pg_bench")]
#[pg_schema]
mod benches {
    use crate::bench_common::{generate_sample_video, sample_video_bytes};
    use pgrx::pg_bench;
    use pgrx_bench::{black_box, Bencher};

    #[pg_bench(setup = generate_sample_video)]
    fn bench_transcode_remux(b: &mut Bencher) {
        let data = sample_video_bytes();
        b.iter(|| black_box(super::transcode(data.clone(), Some("matroska"), None)));
    }

    #[pg_bench(setup = generate_sample_video)]
    fn bench_transcode_filter_scale(b: &mut Bencher) {
        let data = sample_video_bytes();
        b.iter(|| black_box(super::transcode(data.clone(), None, Some("scale=320:240"))));
    }
}
