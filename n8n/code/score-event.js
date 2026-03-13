function text(value) {
  return (value || '').toString().toLowerCase();
}

function includesAny(haystack, needles) {
  return needles.some((needle) => haystack.includes(needle));
}

function isInstitutionalHolder(personName = '', rawSummary = '') {
  const haystack = `${personName} ${rawSummary}`.toLowerCase();
  const institutions = [
    'blackrock',
    'norges bank',
    'ubs',
    'goldman sachs',
    'barclays',
    'state street',
    'jpmorgan',
    'morgan stanley',
    'bank of america',
    'legal & general',
    'allianz',
    'fidelity',
    'vanguard',
    'amundi',
    'invesco',
    'capital group'
  ];
  return includesAny(haystack, institutions);
}

return items.map((item) => {
  const row = item.json;
  const summary = text(row.raw_summary);
  const subclass = text(row.event_subclass);
  const direction = text(row.transaction_direction);
  const person = text(row.person_name_normalized);
  const role = text(row.role_title);
  const estimatedValue = Number(row.estimated_value || 0);

  let ruleScore = 0;
  let liquidityScore = 0;
  let confidenceScore = 1;
  let rejectionReason = null;
  const reasons = [];

  if (row.source === 'afm_mar19') {
    if (subclass === 'sale_or_disposal' || direction === 'outflow') {
      ruleScore += 4;
      reasons.push('mar19 disposal signal');
    } else if (subclass === 'exercise_or_conversion') {
      ruleScore -= 2;
      reasons.push('technical exercise_or_conversion');
    } else if (subclass === 'grant_or_award') {
      ruleScore -= 4;
      reasons.push('grant_or_award');
    } else {
      ruleScore += 1;
      reasons.push('mar19 weak trigger');
    }
  }

  if (row.source === 'afm_substantial_holdings') {
    if (subclass === 'ownership_reduction' || direction === 'outflow') {
      ruleScore += 5;
      reasons.push('ownership reduction');
    } else if (subclass === 'ownership_increase' || direction === 'inflow') {
      ruleScore -= 1;
      reasons.push('ownership increase');
    } else {
      ruleScore += 1;
      reasons.push('ownership change');
    }
  }

  if (row.source === 'euronext_news') {
    if (includesAny(summary, ['major shareholding', 'mandatory notification of trade'])) {
      ruleScore += 3;
      reasons.push('euronext core topic');
    }
    if (includesAny(summary, ['merger', 'acquisition', 'takeover', 'secondary', 'block trade', 'placement', 'transfer'])) {
      ruleScore += 2;
      reasons.push('euronext transaction context');
    }
  }

  if (row.source === 'exa_private_liquidity') {
    if (includesAny(summary, ['sold stake', 'exit', 'management buyout', 'secondary sale', 'dividend recap', 'partial exit'])) {
      ruleScore += 4;
      reasons.push('private liquidity signal');
    } else {
      ruleScore += 1;
      reasons.push('weak private signal');
    }
  }

  if (estimatedValue >= 1000000) {
    liquidityScore += 1;
    reasons.push('estimated value above 1m');
  }

  if (estimatedValue >= 5000000) {
    liquidityScore += 2;
    reasons.push('estimated value above 5m');
  }

  if (includesAny(role, ['chief', 'ceo', 'cfo', 'coo', 'director', 'founder', 'partner', 'bestuurder', 'commissaris'])) {
    confidenceScore += 1;
    reasons.push('senior role');
  }

  if (!row.person_name_normalized && row.source !== 'euronext_news') {
    confidenceScore -= 1;
    reasons.push('missing person signal');
  }

  if (isInstitutionalHolder(person, summary)) {
    ruleScore -= 5;
    rejectionReason = 'institutional_holder_noise';
    reasons.push('institutional holder noise');
  }

  if (includesAny(summary, ['grant', 'award', 'omwisseling', 'uitoefening', 'option exercise']) && !includesAny(summary, ['sale', 'verkoop', 'disposal', 'vervreemding'])) {
    ruleScore -= 4;
    rejectionReason = rejectionReason || 'technical_event_only';
    reasons.push('technical event only');
  }

  if (includesAny(summary, ['buyback', 'share buyback program', 'capital reduction announcement']) && !includesAny(summary, ['founder', 'director', 'holder'])) {
    ruleScore -= 3;
    rejectionReason = rejectionReason || 'governance_noise';
    reasons.push('governance noise');
  }

  const total = ruleScore + liquidityScore + confidenceScore;
  const keepFlag = total >= 6 && !rejectionReason;

  return {
    json: {
      normalized_event_id: row.id || row.normalized_event_id || null,
      issuer_name_normalized: row.issuer_name_normalized || null,
      person_name_normalized: row.person_name_normalized || null,
      event_date: row.event_date || null,
      source: row.source || null,
      event_class: row.event_class || null,
      event_subclass: row.event_subclass || null,
      rule_score: ruleScore,
      liquidity_score: liquidityScore,
      confidence_score: confidenceScore,
      keep_flag: keepFlag,
      rejection_reason: rejectionReason,
      explanation: reasons.join('; ')
    }
  };
});
