function toBool(expr) {
  return `case
    when lower(trim(cast(${expr} as string))) in ('true', 't', '1', 'yes', 'y') then true
    when lower(trim(cast(${expr} as string))) in ('false', 'f', '0', 'no', 'n') then false
    else null
  end`;
}

module.exports = { toBool };
