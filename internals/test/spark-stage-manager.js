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

// const STAGE_REGEXP = /(\[Stage [^\]]+\]).*/;
const STAGE_REGEXP = new RegExp('^\\[Stage (\\d+)[^\\]*]+\\].*$', 'gm');

function matchAll(input, regex) {
  const matches = [];
  let currentInput = input;
  let continueSearching = true;

  while (continueSearching) {
    const match = currentInput.match(regex);
    if (!match) {
      continueSearching = false;
      break;
    }
    matches.push(match);
    currentInput = currentInput.slice(match.index + match[0].length);
  }

  return matches;
}

class SparkStageManager {
  constructor() {
    this.currentChunkAgg = '';
    this.currentStage = null;
  }
  processChunk(chunk) {
    this.currentChunkAgg += chunk.toString('utf8');

    const groups = matchAll(this.currentChunkAgg, STAGE_REGEXP);

    if (groups.length === 0) {
      return;
    }

    this.currentChunkAgg = '';
    const lastGroup = _.last(groups);

    // console.log(`>>> Current stage = ${lastGroup[1]}`);

    this.currentStage = lastGroup[1];
  }
}

module.exports = SparkStageManager;
