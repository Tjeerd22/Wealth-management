function toNumber(value) {
  if (value === null || value === undefined || value === '') return null;
  if (typeof value === 'number') return Number.isFinite(value) ? value : null;
  const cleaned = String(value)
    .replace(/\./g, '')
    .replace(',', '.')
    .replace(/[^\d.-]/g, '');
  const parsed = Number(cleaned);
  return Number.isFinite(parsed) ? parsed : null;
}

function normalizeName(value) {
  if (!value) return null;
  return String(value)
    .trim()
    .replace(/\s+/g, ' ')
    .replace(/N\.V\./gi, 'NV')
    .replace(/B\.V\./gi, 'BV');
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
  if (text.includes('vervreemding') || text.includes('verkoop') || text.includes('sale') || text.includes('disposal')) return 'sale_or_disposal';
  if (text.includes('verwerving') || text.includes('acquisition') || text.includes('purchase')) return 'acquisition';
  if (text.includes('uitoefening') || text.includes('omwisseling') || text.includes('exercise')) return 'exercise_or_conversion';
  if (text.includes('grant') || text.includes('award')) return 'grant_or_award';
  return 'other';
}

function inferOwnershipSubclass(text) {
  if (text.includes('disposal') || text.includes('reduction') || text.includes('decrease') || text.includes('lower')) return 'ownership_reduction';
  if (text.includes('acquisition') || text.includes('increase') || text.includes('higher')) return 'ownership_increase';
  return 'ownership_change';
}

function inferNewsClass(text) {
  if (text.includes('major shareholding')) return 'ownership_shift';
  if (text.includes('mandatory notification of trade')) return 'insider_transaction';
  if (text.includes('merger') || text.includes('acquisition') || text.includes('takeover') || text.includes('transfer')) return 'corporate_transaction';
  if (text.includes('financial transaction') || text.includes('placement') || text.includes('secondary')) return 'corporate_transaction';
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
  if (/(sale|verkoop|disposal|vervreemding|reduction|decrease)/.test(haystack)) return 'outflow';
  if (/(acquisition|purchase|verwerving|increase)/.test(haystack)) return 'inflow';
  return 'unknown';
}

const input = items.map((item) => item.json);

return input.map((row) => {
  const source = row.source || 'unknown';
  const issuer = normalizeName(row.issuer_name || row['Uitgevende instelling'] || row.issuer || row.company);
  const person = normalizeName(row.person_name || row['Meldingsplichtige'] || row['Person obliged to notify'] || row.notifiable);
  const title = row.title || '';
  const rawText = row.raw_text || row.body || row.description || '';
  const roleTitle = row.role_title || row['Positie'] || row.position || null;
  const price = toNumber(row.price || row['Prijs'] || row.transaction_price);
  const quantity = toNumber(row.quantity || row['Aantal'] || row.transaction_quantity);
  const estimatedValue = price !== null && quantity !== null ? Number((price * quantity).toFixed(2)) : null;
  const inferred = inferEventClass(source, title, rawText);
  const direction = inferDirection(`${title} ${rawText} ${row.transaction_type || ''} ${row.transaction_category || ''}`);

  return {
    json: {
      raw_event_id: row.id || row.raw_event_id || null,
      issuer_name_normalized: issuer,
      person_name_normalized: person,
      event_date: row.event_date || row['Transactie'] || row['Date of transaction'] || null,
      source,
      event_class: inferred.eventClass,
      event_subclass: inferred.eventSubclass,
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
