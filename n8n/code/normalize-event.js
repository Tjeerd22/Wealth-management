function toNumber(value) {
  if (value === null || value === undefined || value === '') return null;
  if (typeof value === 'number') return Number.isFinite(value) ? value : null;
  const cleaned = String(value)
    .trim()
    .replace(/\s+/g, '')
    .replace(/\.(?=\d{3}(\D|$))/g, '')
    .replace(',', '.')
    .replace(/[^\d.-]/g, '');
  const parsed = Number(cleaned);
  return Number.isFinite(parsed) ? parsed : null;
}

function parseDate(value) {
  if (!value) return null;
  const raw = String(value).trim();
  if (!raw) return null;
  if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) return raw;

  const normalized = raw.replace(/\./g, '-').replace(/\//g, '-');
  const parts = normalized.split('-').map((part) => part.trim());
  if (parts.length === 3) {
    if (parts[0].length === 4) {
      const [y, m, d] = parts;
      if (y && m && d) return `${y.padStart(4, '0')}-${m.padStart(2, '0')}-${d.padStart(2, '0')}`;
    }
    const [d, m, y] = parts;
    if (y && m && d) return `${y.padStart(4, '0')}-${m.padStart(2, '0')}-${d.padStart(2, '0')}`;
  }

  const parsed = new Date(raw);
  if (!Number.isNaN(parsed.getTime())) {
    return parsed.toISOString().slice(0, 10);
  }
  return null;
}

function normalizeName(value) {
  if (!value) return null;
  return String(value)
    .trim()
    .replace(/\s+/g, ' ')
    .replace(/\bN\.V\.\b/gi, 'NV')
    .replace(/\bB\.V\.\b/gi, 'BV')
    .replace(/^\W+|\W+$/g, '') || null;
}

function inferEventClass(source, title = '', rawText = '') {
  const haystack = `${title} ${rawText}`.toLowerCase();

  if (source === 'afm_mar19') return { eventClass: 'insider_transaction', eventSubclass: inferTransactionSubclass(haystack) };
  if (source === 'afm_substantial_holdings') return { eventClass: 'ownership_shift', eventSubclass: inferOwnershipSubclass(haystack) };
  if (source === 'euronext_news') return { eventClass: inferNewsClass(haystack), eventSubclass: inferNewsSubclass(haystack) };
  if (source === 'afm_inside_information') return { eventClass: 'inside_information', eventSubclass: inferNewsSubclass(haystack) };
  if (source === 'exa_private_liquidity') return { eventClass: 'private_liquidity', eventSubclass: inferNewsSubclass(haystack) };
  return { eventClass: 'unknown', eventSubclass: 'unknown' };
}

function inferTransactionSubclass(text) {
  if (/(vervreemding|verkoop|sale|disposal|sold|afbouw)/.test(text)) return 'sale_or_disposal';
  if (/(verwerving|acquisition|purchase|koop)/.test(text)) return 'acquisition';
  if (/(uitoefening|omwisseling|exercise|conversion)/.test(text)) return 'exercise_or_conversion';
  if (/(grant|award|toekenning)/.test(text)) return 'grant_or_award';
  return 'other';
}

function inferOwnershipSubclass(text) {
  if (/(disposal|reduction|decrease|lower|afname|vermindering|below|onderschrijding)/.test(text)) return 'ownership_reduction';
  if (/(acquisition|increase|higher|stijging|toename|boven)/.test(text)) return 'ownership_increase';
  return 'ownership_change';
}

function inferNewsClass(text) {
  if (text.includes('major shareholding')) return 'ownership_shift';
  if (text.includes('mandatory notification of trade')) return 'insider_transaction';
  if (/(merger|acquisition|takeover|transfer|placement|secondary|block trade)/.test(text)) return 'corporate_transaction';
  return 'news_event';
}

function inferNewsSubclass(text) {
  if (text.includes('secondary')) return 'secondary_sale';
  if (text.includes('block trade')) return 'block_trade';
  if (text.includes('placement')) return 'placement';
  if (text.includes('takeover')) return 'takeover';
  if (text.includes('merger')) return 'merger';
  if (text.includes('acquisition')) return 'acquisition';
  if (text.includes('shareholding')) return 'shareholding_notification';
  return 'other';
}

function inferDirection(text = '') {
  const haystack = text.toLowerCase();
  if (/(sale|verkoop|disposal|vervreemding|reduction|decrease|afname|vermindering|sold|exit)/.test(haystack)) return 'outflow';
  if (/(acquisition|purchase|verwerving|increase|toename|buy|invest)/.test(haystack)) return 'inflow';
  return 'unknown';
}

const input = items.map((item) => item.json);

return input.map((row) => {
  const source = row.source || 'unknown';
  const issuer = normalizeName(row.issuer_name || row['Uitgevende instelling'] || row['Issuing institution'] || row.issuer || row.company);
  const person = normalizeName(row.person_name || row['Meldingsplichtige'] || row['Person obliged to notify'] || row.notifiable);
  const title = row.title || '';
  const rawText = row.raw_text || row.body || row.description || '';
  const roleTitle = row.role_title || row['Positie'] || row.position || null;
  const price = toNumber(row.price || row['Prijs'] || row.transaction_price);
  const quantity = toNumber(row.quantity || row['Aantal'] || row.transaction_quantity);
  const estimatedValue = row.estimated_value !== undefined && row.estimated_value !== null
    ? toNumber(row.estimated_value)
    : (price !== null && quantity !== null ? Number((price * quantity).toFixed(2)) : null);
  const inferred = inferEventClass(source, title, rawText);
  const direction = row.transaction_direction || inferDirection(`${title} ${rawText} ${row.transaction_type || ''} ${row.transaction_category || ''}`);

  return {
    json: {
      raw_event_id: row.id || row.raw_event_id || null,
      issuer_name_normalized: issuer,
      person_name_normalized: person,
      event_date: parseDate(row.event_date || row['Transactie'] || row['Date of transaction']) || null,
      source,
      event_class: row.event_class || inferred.eventClass,
      event_subclass: row.event_subclass || inferred.eventSubclass,
      transaction_direction: direction,
      price,
      quantity,
      currency: row.currency || row['Eenheid'] || row.unit || null,
      estimated_value: estimatedValue,
      role_title: roleTitle,
      detail_status: row.detail_status || 'normalized',
      source_url: row.detail_url || row.source_url || null,
      raw_summary: rawText ? String(rawText).slice(0, 1200) : title || null,
    }
  };
});
