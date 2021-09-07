#!/usr/bin/env node

import { Writable, WritableOptions } from 'stream'

import { TSPacket, TSPacketQueue } from 'arib-mpeg2ts-parser';
import { TSSection, TSSectionQueue, TSSectionPacketizer } from 'arib-mpeg2ts-parser';
import { TSPES, TSPESQueue } from 'arib-mpeg2ts-parser';

class PartialSegment {
  private beginPTS: number;
  private endPTS: number | null;
  private hasIFrame: boolean;
  private completed: Buffer;
  private retains: Buffer[];
  private complete_callbacks: ((this: PartialSegment) => void)[];

  constructor (beginPTS: number, hasIFrame?: boolean) {
    this.beginPTS = beginPTS;
    this.endPTS = null;
    this.hasIFrame = hasIFrame ?? false;
    this.completed = Buffer.from([]);
    this.retains = [];
    this.complete_callbacks = [];
  }

  public addPacket(packet: Buffer): void {
    this.retains.push(packet);
  }

  public addCallback(cb: (() => void)): void {
    this.complete_callbacks.push(cb);
  }

  public isCompleted(): boolean {
    return this.endPTS != null;
  }

  public complete(endPTS: number): void {
    this.endPTS = endPTS;

    this.completed = Buffer.concat([this.completed, ... this.retains]);
    this.retains = [];

    this.complete_callbacks.forEach((cb) => cb.call(this));
    this.complete_callbacks = [];
  }

  public getBuffer(): Buffer {
    return this.completed;
  }

  public getSeconds(): number | null {
    if (this.endPTS == null) { return null; }

    return (this.endPTS - this.beginPTS + (2 ** 33)) % (2 ** 33) / 90000;
  }

  public estimateSeconds(endPTS: number): number {
    return (endPTS - this.beginPTS + (2 ** 33)) % (2 ** 33) / 90000;
  }

  public getHasIFrame(): boolean {
    return this.hasIFrame;
  }
}

class Segment extends PartialSegment {
  private parts: PartialSegment[] = [];
  private programDateTime: string;

  constructor (beginPTS: number, hasIFrame?: boolean) {
    super(beginPTS, hasIFrame);
    this.newPartial(beginPTS, hasIFrame);

    this.programDateTime = new Date().toISOString();
  }

  public complete(endPTS: number) {
    super.complete(endPTS);
    this.completePartial(endPTS);
  }

  public addPacket(packet: Buffer){
    super.addPacket(packet);

    const lastPart = this.parts[this.parts.length - 1]
    lastPart.addPacket(packet);
  }

  public getLength() {
    return this.parts.length;
  }

  public getPartial(index: number): PartialSegment | undefined {
    return this.parts[index]
  }

  public getProgramDateTime() {
    return this.programDateTime;
  }

  public completePartial(endPTS: number) {
    if (this.parts.length === 0) { return; }

    const lastPart = this.parts[this.parts.length - 1];
    lastPart.complete(endPTS);
  }

  public newPartial(beginPTS: number, hasIFrame?: boolean) {
    this.parts.push(new PartialSegment(beginPTS, hasIFrame));
  }
}

type TSFragmenterOptions = WritableOptions & {
  length?: number,
  partTarget?: number,
}

export default class TSFragmenter extends Writable {
  private packetQueue = new TSPacketQueue();

  private Packet_TransportErrorIndicator: boolean = false;
  private Packet_TransportPriority: boolean = false;
  private Packet_TransportScramblingControl: number = 0;

  private PAT_TSSectionQueue = new TSSectionQueue();
  private PAT_lastPAT: Buffer | null = null;
  private PAT_Continuous_Counter: number = 0;
  private PAT_TargetSID: number | null = null;
  private PAT_TargetPMTPid: number | null = null;

  private PMT_TSSectionQueue = new TSSectionQueue();
  private PMT_lastPMT: Buffer | null = null;
  private PMT_Continuous_Counter: number = 0;
  private PMT_VideoPid: number | null = null;
  private PMT_PCRPid: number | null = null;

  private Video_TSPESQueue = new TSPESQueue();
  private Video_Packets: Buffer[] = [];

  private M3U8_Segments_Length: number;
  private M3U8_Part_Target: number;
  private M3U8_Begin_Sequence_Number: number = 0;
  private M3U8_End_Sequence_Number: number = 0;
  private M3U8_Segments: Segment[] = [];

  private M3U8_Initial_PAT_Detected: boolean = false;
  private M3U8_Initial_PMT_Detected: boolean = false;
  private M3U8_Initial_IDR_Detected: boolean = false;

  public constructor(options?: TSFragmenterOptions) {
    super(options);
    this.M3U8_Segments_Length = options?.length ?? 3;
    this.M3U8_Part_Target = options?.partTarget ?? 1;
  }

  public getManifest(): string {
    let m3u8 = '';

    m3u8 += `#EXTM3U\n`
    m3u8 += `#EXT-X-VERSION:6\n`
    m3u8 += `#EXT-X-TARGETDURATION:${this.getTargetDuration()}\n`
    m3u8 += `#EXT-X-PART-INF:PART-TARGET=${this.M3U8_Part_Target.toFixed(3)}\n`
    m3u8 += `#EXT-X-SERVER-CONTROL:CAN-BLOCK-RELOAD=YES,PART-HOLD-BACK=${(this.M3U8_Part_Target * 3).toFixed(3)}\n`
    m3u8 += `#EXT-X-MEDIA-SEQUENCE:${this.M3U8_Begin_Sequence_Number}\n`
    for (let media_sequence = this.M3U8_Begin_Sequence_Number; media_sequence < this.M3U8_End_Sequence_Number; media_sequence++) {
      const index = media_sequence - this.M3U8_Begin_Sequence_Number;
      const segment = this.M3U8_Segments[index];
      let extinf = 0;

      m3u8 += `\n`;
      m3u8 += `#EXT-X-PROGRAM-DATE-TIME:${segment.getProgramDateTime()}\n`
      for (let p = 0; p < segment.getLength(); p++) {
        const part = segment.getPartial(p);
        if (!part) { break; }

        // FIXME: segment 完了通知が飛んだ際に、ここでまだ part が終了してない。なんで、segment で待つのは推奨しない状態になってる。
        if (!part.isCompleted()) {
          m3u8 += `#EXT-X-PRELOAD-HINT:TYPE=PART,URI="part?msn=${media_sequence}&part.ts=${p}"${part.getHasIFrame() ? ",INDEPENDENT=YES" : ""}\n`
        } else {
          m3u8 += `#EXT-X-PART:DURATION=${part.getSeconds()!.toFixed(3)},URI="part?msn=${media_sequence}&part=${p}"${part.getHasIFrame() ? ",INDEPENDENT=YES" : ""}\n`
          extinf += Number.parseFloat(part.getSeconds()?.toFixed(3) ?? "0");
        }
      }

      if (segment.isCompleted()) {
        m3u8 += `#EXTINF:${extinf.toFixed(3)}\n`
        m3u8 += `segment?msn=${media_sequence}\n`;
      }
    }

    return m3u8
  }

  private getTargetDuration() {
    let max = 1;
    for (let media_sequence = this.M3U8_Begin_Sequence_Number; media_sequence < this.M3U8_End_Sequence_Number; media_sequence++) {
      const index = media_sequence - this.M3U8_Begin_Sequence_Number;
      const segment = this.M3U8_Segments[index];
      max = Math.max(max, segment.getSeconds() ?? 0);
    }

    return Math.ceil(max)
  }

  public inRangeSegment(msn: number): boolean {
    if (msn < this.M3U8_Begin_Sequence_Number) { return false; }
    if (this.M3U8_End_Sequence_Number <= msn) { return false; }
    return true;
  }
  public isFulfilledSegment(msn: number): boolean {
    const segment = this.M3U8_Get_Segment(msn);
    if (!segment) { return true; }

    return segment.isCompleted();
  }
  public getSegment(msn: number): Buffer {
    const segment = this.M3U8_Get_Segment(msn);
    if (!segment) { return Buffer.from([]); }

    return segment.getBuffer();
  }
  public addSegmentCallback(msn: number, cb: () => void): boolean {
    const segment = this.M3U8_Get_Segment(msn);
    if (!segment) { return false; }

    segment.addCallback(cb);

    return true;
  }

  public inRangePartial(msn: number, part: number): boolean {
    if (!this.inRangeSegment(msn)) { return false; }

    const index = msn - this.M3U8_Begin_Sequence_Number;
    const segment = this.M3U8_Segments[index];
    const partial = segment.getPartial(part);
    if (!partial) { return false; }

    return true;
  }
  public isFulfilledPartial(msn: number, part: number): boolean {
    const partial = this.M3U8_Get_Partial(msn, part);
    if (!partial) { return true; }

    return partial.isCompleted();
  }
  public getPartial(msn: number, part: number): Buffer {
    const partial = this.M3U8_Get_Partial(msn, part);
    if (!partial) { return Buffer.from([]); }

    return partial.getBuffer();
  }
  public addPartialCallback(msn: number, part: number, cb: () => void) {
    const partial = this.M3U8_Get_Partial(msn, part);
    if (!partial) { return false; }

    partial.addCallback(cb);

    return true;
  }

  private M3U8_Add_PAT(PAT: Buffer) {
    const packets = TSSectionPacketizer.packetize(
      PAT,
      this.Packet_TransportErrorIndicator,
      this.Packet_TransportPriority,
      0,
      this.Packet_TransportScramblingControl,
      this.PAT_Continuous_Counter
    );
    this.PAT_Continuous_Counter = (this.PAT_Continuous_Counter + packets.length) & 0x0F;

    packets.forEach((packet) => this.M3U8_Add_Packet(packet));
  }

  private M3U8_Add_PMT(PMT: Buffer) {
    if (!this.PAT_TargetPMTPid) { return }

    const packets = TSSectionPacketizer.packetize(
      PMT,
      this.Packet_TransportErrorIndicator,
      this.Packet_TransportPriority,
      this.PAT_TargetPMTPid,
      this.Packet_TransportScramblingControl,
      this.PMT_Continuous_Counter
    );
    this.PMT_Continuous_Counter = (this.PMT_Continuous_Counter + packets.length) & 0x0F;

    packets.forEach((packet) => this.M3U8_Add_Packet(packet));
  }

  private M3U8_Add_Packet(packet: Buffer) {
    const size = this.M3U8_End_Sequence_Number - this.M3U8_Begin_Sequence_Number;
    if (size <= 0) {  return; }

    const lastSegment = this.M3U8_Segments[size - 1];
    lastSegment.addPacket(packet);
  }

  private M3U8_Get_Segment(msn: number): Segment | null {
    if (!this.inRangeSegment(msn)) { return null; }

    const index = msn - this.M3U8_Begin_Sequence_Number;
    const segment = this.M3U8_Segments[index];
    return segment;
  }

  private M3U8_Get_Partial(msn: number, part: number): PartialSegment | null {
    if (!this.inRangePartial(msn, part)) { return null; }

    const index = msn - this.M3U8_Begin_Sequence_Number;
    const segment = this.M3U8_Segments[index];
    const partial = segment.getPartial(part);
    if (!partial) { return null; }

    return partial;
  }

  private M3U8_Get_Last_Partial() {
    const size = this.M3U8_End_Sequence_Number - this.M3U8_Begin_Sequence_Number;
    if (size <= 0) { return null; }

    const lastSegment = this.M3U8_Segments[size - 1];
    const length = lastSegment.getLength();

    const lastPart = lastSegment.getPartial(length - 1);
    return lastPart;
  }

  private M3U8_New_Partial(beginPTS: number) {
    const size = this.M3U8_End_Sequence_Number - this.M3U8_Begin_Sequence_Number;
    if (size <= 0) {  return; }

    const lastSegment = this.M3U8_Segments[size - 1];
    lastSegment.completePartial(beginPTS);
    lastSegment.newPartial(beginPTS);
  }

  private M3U8_New_Segment(beginPTS: number) {
    if (!this.PAT_lastPAT) { return; }
    if (!this.PMT_lastPMT) { return; }

    // 完了通知を飛ばしておく
    {
      const size = this.M3U8_End_Sequence_Number - this.M3U8_Begin_Sequence_Number;
      if (size > 0) { // not is Empty
        const lastSegment = this.M3U8_Segments[size - 1];
        lastSegment.complete(beginPTS);
      }
    }

    // 新規セグメント追加
    this.M3U8_Segments.push(new Segment(beginPTS, true));
    this.M3U8_End_Sequence_Number += 1;

    // 消し込み
    {
      const size = this.M3U8_End_Sequence_Number - this.M3U8_Begin_Sequence_Number;
      if (size > this.M3U8_Segments_Length) {
        this.M3U8_Segments.shift();
        this.M3U8_Begin_Sequence_Number += 1;
      }
    }

    // 作ったセグメントの先頭に PAT, PMT を追加しないと仕様に違反する
    this.M3U8_Add_PAT(this.PAT_lastPAT);
    this.M3U8_Add_PMT(this.PMT_lastPMT);
  }

  _write(chunk: Buffer, encoding: 'Buffer', callback: (error?: Error | null) => void): void {
    this.packetQueue.push(chunk);

    while (!this.packetQueue.isEmpty()) {
      const packet = this.packetQueue.pop()!;

      const pid = TSPacket.pid(packet);

      this.Packet_TransportErrorIndicator = TSPacket.transport_error_indicator(packet);
      this.Packet_TransportPriority = TSPacket.transport_priority(packet);
      this.Packet_TransportScramblingControl = TSPacket.transport_scrambling_control(packet);

      if (pid == 0x00) {
        this.PAT_TSSectionQueue.push(packet)
        while (!this.PAT_TSSectionQueue.isEmpty()) {
          const PAT = this.PAT_TSSectionQueue.pop()!;
          if (TSSection.CRC32(PAT) != 0) { continue; }

          this.PAT_lastPAT = PAT;
          this.PAT_TargetPMTPid = null;

          let begin = TSSection.EXTENDED_HEADER_SIZE;
          while (begin < TSSection.BASIC_HEADER_SIZE + TSSection.section_length(PAT) - TSSection.CRC_SIZE) {
            const program_number = (PAT[begin + 0] << 8) | PAT[begin + 1];
            const program_map_PID = ((PAT[begin + 2] & 0x1F) << 8) | PAT[begin + 3];
            if (program_map_PID === 0x10) { begin += 4; continue; } // NIT

            if (this.PAT_TargetSID === program_number) {
              this.PAT_TargetPMTPid = program_map_PID;
            } else if (this.PAT_TargetSID == null && this.PAT_TargetPMTPid == null) {
              this.PAT_TargetPMTPid = program_map_PID;
            }

            begin += 4;
          }

          if (!this.M3U8_Initial_PAT_Detected) {
            this.M3U8_Initial_PAT_Detected = true;
          }

          if (this.M3U8_Initial_IDR_Detected) {
            this.M3U8_Add_PAT(PAT);
          }
        }
      } else if (pid === this.PAT_TargetPMTPid) {
        this.PMT_TSSectionQueue.push(packet);

        while (!this.PMT_TSSectionQueue.isEmpty()) {
          const PMT = this.PMT_TSSectionQueue.pop()!;
          if (TSSection.CRC32(PMT) != 0) { continue; }

          this.PMT_lastPMT = PMT;
          this.PMT_VideoPid = null;

          const PCR_PID = ((PMT[TSSection.EXTENDED_HEADER_SIZE + 0] & 0x1F) << 8) | PMT[TSSection.EXTENDED_HEADER_SIZE + 1];
          this.PMT_PCRPid = PCR_PID;

          const program_info_length = ((PMT[TSSection.EXTENDED_HEADER_SIZE + 2] & 0x0F) << 8) | PMT[TSSection.EXTENDED_HEADER_SIZE + 3];

          let begin = TSSection.EXTENDED_HEADER_SIZE + 4 + program_info_length;
          while (begin < TSSection.BASIC_HEADER_SIZE + TSSection.section_length(PMT) - TSSection.CRC_SIZE) {
            const stream_type = PMT[begin + 0];
            const elementary_PID = ((PMT[begin + 1] & 0x1F) << 8) | PMT[begin + 2];
            const ES_info_length = ((PMT[begin + 3] & 0x0F) << 8) | PMT[begin + 4];

            if (this.PMT_VideoPid == null && stream_type === 0x1b) { // AVC VIDEO
              this.PMT_VideoPid = elementary_PID;
            }

            begin += 5 + ES_info_length;
          }

          if (!this.M3U8_Initial_PMT_Detected) {
            this.M3U8_Initial_PMT_Detected = true;
          }

          if (this.M3U8_Initial_IDR_Detected) {
            this.M3U8_Add_PMT(PMT);
          }
        }
      } else if (pid === this.PMT_VideoPid) {
        this.Video_TSPESQueue.push(packet);
        this.Video_Packets.push(packet);

        while (!this.Video_TSPESQueue.isEmpty()) {
          const VideoPES = this.Video_TSPESQueue.pop()!;

          let pts = 0;
          pts *= (1 << 3); pts += ((VideoPES[TSPES.PES_HEADER_SIZE + 3 + 0] & 0x0E) >> 1);
          pts *= (1 << 8); pts += ((VideoPES[TSPES.PES_HEADER_SIZE + 3 + 1] & 0xFF) >> 0);
          pts *= (1 << 7); pts += ((VideoPES[TSPES.PES_HEADER_SIZE + 3 + 2] & 0xFE) >> 1);
          pts *= (1 << 8); pts += ((VideoPES[TSPES.PES_HEADER_SIZE + 3 + 3] & 0xFF) >> 0);
          pts *= (1 << 7); pts += ((VideoPES[TSPES.PES_HEADER_SIZE + 3 + 4] & 0xFE) >> 1);

          const PES_header_data_length = VideoPES[TSPES.PES_HEADER_SIZE + 2];

          let hasIDR = false;

          let begin = TSPES.PES_HEADER_SIZE + 2 + PES_header_data_length;
          while (begin < VideoPES.length) {
            if (begin + 2 >= VideoPES.length) { break; }

            if (VideoPES[begin + 0] !== 0) { begin += 1; continue; }
            if (VideoPES[begin + 1] !== 0) { begin += 1; continue; }
            if (VideoPES[begin + 2] !== 1) { begin += 1; continue; }

            if (begin + 3 >= VideoPES.length) { break; }
            const nal_unit_type = VideoPES[begin + 3] & 0x1f;
            if (nal_unit_type === 5) {
              hasIDR = true;
              break;
            }
            begin += 4;
          }

          if (hasIDR) {
            if (!this.M3U8_Initial_IDR_Detected) {
              this.M3U8_Initial_IDR_Detected = true;
            }

            this.M3U8_New_Segment(pts);
          }

          if (this.M3U8_Initial_IDR_Detected) {
            if (!hasIDR) {
              const part = this.M3U8_Get_Last_Partial();
              if (part) {
                const time = part.estimateSeconds(pts);

                if (this.M3U8_Part_Target * 0.85 < time && time <= this.M3U8_Part_Target) {
                  this.M3U8_New_Partial(pts);
                }
              }
            }

            this.Video_Packets.forEach((packet) => this.M3U8_Add_Packet(packet));
            this.Video_Packets = [];
          }
        }
      } else if (pid === this.PMT_PCRPid) {
        if (TSPacket.has_pcr(packet)) {
          const PCR = TSPacket.pcr(packet);
        }

        if (this.M3U8_Initial_IDR_Detected) {
          this.M3U8_Add_Packet(packet);
        }
      } else {
        if (this.M3U8_Initial_IDR_Detected) {
          this.M3U8_Add_Packet(packet);
        }
      }
    }

    callback();
  }
}
