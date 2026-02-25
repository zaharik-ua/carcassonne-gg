function collectMatchPayloads(data, rowNumbers, options = {}) {
  const { 
    requireStat = false,
    filterStarted = false,
    filterInFuture = false,
    filterOngoing = false,
    filterFinished = false,
    filterNotScored = false,
    filterEmpty = false
  } = options;

  const payloads = [];
  const rowIndexes = [];

  const now = Math.floor(Date.now() / 1000);

  for (const rowIndex of rowNumbers) {
    const row = data[rowIndex - 1];
    const startDate = row[14];
    const endDate = row[24];
    const player1 = row[16];
    const player1Id = row[15];
    const player2 = row[19];
    const player2Id = row[20];
    const status = row[25];
    const stat1 = row[31];
    const stat2 = row[32];
    const score1 = row[17];
    const score2 = row[18];
    const gamesToWin = row[28];
    if (requireStat) {
      if (status == "IN PROGRESS") { return }
      if (stat1 !== '' || stat2 !== '') continue;
    }

    if (!player1 || !player2 || !player1Id || !player2Id || !startDate || !endDate) continue;

    const startTimestamp = toUtcTimestamp(startDate);
    const endTimestamp = toUtcTimestamp(endDate);

    if (filterStarted && startTimestamp > now) continue;
    if (filterInFuture && startTimestamp < now) continue;
    if (filterOngoing && endTimestamp < now) continue;
    if (filterFinished && endTimestamp > now) continue;
    if (filterNotScored && (score1 == gamesToWin || score2 == gamesToWin)) continue;
    if (filterEmpty && (score1 !== "" && score1 !== null && score2 !== "" && score2 !== null)) continue;

    payloads.push({
      player0: player1,
      player1: player2,
      player0_id: player1Id.toString(),
      player1_id: player2Id.toString(),
      game_id: 1,
      gtw: gamesToWin,
      stat: requireStat,
      start_date: requireStat ? startTimestamp - 63244800 : startTimestamp,
      end_date: endTimestamp
    });

    rowIndexes.push(rowIndex);
  }

  return { payloads, rowIndexes };
}