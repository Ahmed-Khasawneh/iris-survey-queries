const _ = require('lodash');
// const exampleText = `
// [Stage 164:(2131 + 124) / 4000][Stage 185:(714 + 206) / 4000][Stage 188:> (5 + 0) / 20]
// [Stage 164:(2131 + 124) / 4000][Stage 185:(723 + 212) / 4000][Stage 188:> (5 + 0) / 20]
// [Stage 164:(2131 + 124) / 4000][Stage 185:(728 + 214) / 4000][Stage 188:> (5 + 0) / 20]
// [Stage 164:(2131 + 124) / 4000][Stage 185:(736 + 216) / 4000][Stage 188:> (5 + 0) / 20]
// [Stage 164:(2131 + 124) / 4000][Stage 185:(740 + 216) / 4000][Stage 188:> (5 + 0) / 20]
// [Stage 164:(2131 + 124) / 4000][Stage 185:(745 + 223) / 4000][Stage 188:> (5 + 0) / 20]
// [Stage 164:(2131 + 124) / 4000][Stage 185:(747 + 226) / 4000][Stage 188:> (5 + 0) / 20]
// [Stage 164:(2131 + 124) / 4000][Stage 185:(754 + 228) / 4000][Stage 188:> (5 + 0) / 20]
// [Stage 164:(2131 + 124) / 4000][Stage 185:(759 + 240) / 4000][Stage 188:> (5 + 0) / 20]
// [Stage 164:(2131 + 124) / 4000][Stage 185:(762 + 237) / 4000][Stage 188:> (5 + 0) / 20]`;

const STAGE_REGEXP = /^\[Stage (\d+)[^\]*]+\].*$/;

class SparkStageManager {
  constructor() {
    this.incompleteLine = '';
    this.currentStage = null;
  }
  processChunk(chunk) {
    // break the chunk into lines (split by newline and/or return character)
    const lines = (this.incompleteLine + chunk.toString('utf8')).split(
      /[\n\r]+/g,
    );
    // assume all but the last lines are complete (last line may have more data coming in later)
    const completeLines = lines.slice(0, -1);
    // assume last line will be completed by a future chunk
    this.incompleteLine = _.last(lines);

    // iterate through all complete lines to see if anything matches
    for (const line of completeLines) {
      const group = line.match(STAGE_REGEXP);

      if (group) {
        this.currentStage = group[1] ? Number(group[1]) : null;
      }
    }
  }
}

module.exports = SparkStageManager;
