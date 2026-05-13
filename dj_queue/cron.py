import calendar
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta, timezone, tzinfo
from functools import lru_cache
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


_MAX_SEARCH_DAYS = 400 * 366
_ONE_DAY = timedelta(days=1)
_INT_RE = re.compile(r"[-+]?\d+\Z")
_OFFSET_RE = re.compile(r"([+-])(\d{2})(?::?(\d{2}))?\Z")
_WEEKDAY_MODULO_RE = re.compile(r"(\d+)(?:\+(\d+))?\Z")
_NATURAL_LIST_RE = re.compile(r"\s*(?:,|\band\b|\bor\b)\s*", re.IGNORECASE)
_REFERENCE_MONDAY = date(2018, 12, 31)

_SPECIALS = {
  "@yearly": "0 0 1 1 *",
  "@annually": "0 0 1 1 *",
  "@monthly": "0 0 1 * *",
  "@weekly": "0 0 * * 0",
  "@daily": "0 0 * * *",
  "@midnight": "0 0 * * *",
  "@noon": "0 12 * * *",
  "@hourly": "0 * * * *",
}
_MONTH_NAMES = {
  "JAN": 1,
  "FEB": 2,
  "MAR": 3,
  "APR": 4,
  "MAY": 5,
  "JUN": 6,
  "JUL": 7,
  "AUG": 8,
  "SEP": 9,
  "OCT": 10,
  "NOV": 11,
  "DEC": 12,
}
_WEEKDAY_NAMES = {
  "SUN": 0,
  "MON": 1,
  "TUE": 2,
  "WED": 3,
  "THU": 4,
  "FRI": 5,
  "SAT": 6,
}
_NATURAL_WEEKDAY_NAMES = {
  "sunday": 0,
  "sun": 0,
  "monday": 1,
  "mon": 1,
  "tuesday": 2,
  "tue": 2,
  "tues": 2,
  "wednesday": 3,
  "wed": 3,
  "thursday": 4,
  "thu": 4,
  "thur": 4,
  "thurs": 4,
  "friday": 5,
  "fri": 5,
  "saturday": 6,
  "sat": 6,
}
_NATURAL_SMALL_NUMBERS = {
  "zero": 0,
  "oh": 0,
  "one": 1,
  "two": 2,
  "three": 3,
  "four": 4,
  "five": 5,
  "six": 6,
  "seven": 7,
  "eight": 8,
  "nine": 9,
  "ten": 10,
  "eleven": 11,
  "twelve": 12,
  "thirteen": 13,
  "fourteen": 14,
  "fifteen": 15,
  "sixteen": 16,
  "seventeen": 17,
  "eighteen": 18,
  "nineteen": 19,
}
_NATURAL_TENS = {
  "twenty": 20,
  "thirty": 30,
  "forty": 40,
  "fifty": 50,
}
_NATURAL_INTERVAL_UNITS = {
  "s": "second",
  "sec": "second",
  "secs": "second",
  "second": "second",
  "seconds": "second",
  "m": "minute",
  "min": "minute",
  "mins": "minute",
  "minute": "minute",
  "minutes": "minute",
  "h": "hour",
  "hour": "hour",
  "hours": "hour",
  "d": "day",
  "day": "day",
  "days": "day",
  "month": "month",
  "months": "month",
  "week": "week",
  "year": "year",
}
_NATURAL_MONTHDAYS = {
  "1st": 1,
  "2nd": 2,
  "3rd": 3,
  "21st": 21,
  "22nd": 22,
  "23rd": 23,
  "31st": 31,
  "first": 1,
  "second": 2,
  "third": 3,
  "fourth": 4,
  "fifth": 5,
  "sixth": 6,
  "seventh": 7,
  "eighth": 8,
  "ninth": 9,
  "tenth": 10,
  "eleventh": 11,
  "twelfth": 12,
  "thirteenth": 13,
  "fourteenth": 14,
  "fifteenth": 15,
  "sixteenth": 16,
  "seventeenth": 17,
  "eighteenth": 18,
  "nineteenth": 19,
  "twentieth": 20,
  "twenty-first": 21,
  "twenty-second": 22,
  "twenty-third": 23,
  "twenty-fourth": 24,
  "twenty-fifth": 25,
  "twenty-sixth": 26,
  "twenty-seventh": 27,
  "twenty-eighth": 28,
  "twenty-ninth": 29,
  "thirtieth": 30,
  "thirty-first": 31,
  "last": "L",
}
_NATURAL_MONTHDAYS.update({f"{day}th": day for day in range(4, 31)})


@dataclass(frozen=True, slots=True)
class _Field:
  values: frozenset[int]
  wildcard: bool

  def matches(self, value: int) -> bool:
    return self.wildcard or value in self.values


@dataclass(frozen=True, slots=True)
class _WeekdaySpec:
  day: int
  nth: int | None = None
  modulo: tuple[int, int] | None = None


@dataclass(frozen=True, slots=True)
class _WeekdayField:
  specs: tuple[_WeekdaySpec, ...]
  wildcard: bool

  def matches(self, moment: datetime) -> bool:
    if self.wildcard:
      return True

    weekday = _cron_weekday(moment)
    for spec in self.specs:
      if spec.day != weekday:
        continue
      if spec.nth is not None and _weekday_hash_matches(moment, spec.nth):
        return True
      if spec.modulo is not None and _weekday_modulo_matches(moment, spec.modulo):
        return True
      if spec.nth is None and spec.modulo is None:
        return True
    return False


@dataclass(frozen=True, slots=True)
class _Schedule:
  seconds: _Field
  minutes: _Field
  hours: _Field
  day_of_month: _Field
  months: _Field
  day_of_week: _WeekdayField
  day_and: bool
  timezone: tzinfo | None

  def matches_minute(self, moment: datetime) -> bool:
    if not self.minutes.matches(moment.minute):
      return False
    if not self.hours.matches(moment.hour):
      return False
    if not self.months.matches(moment.month):
      return False
    if not self._matches_day(moment):
      return False
    return True

  def _matches_day(self, moment: datetime) -> bool:
    day_of_month_matches = self._matches_day_of_month(moment)
    day_of_week_matches = self.day_of_week.matches(moment)

    if self.day_of_month.wildcard and self.day_of_week.wildcard:
      return True
    if self.day_of_month.wildcard:
      return day_of_week_matches
    if self.day_of_week.wildcard:
      return day_of_month_matches
    if self.day_and:
      return day_of_month_matches and day_of_week_matches
    return day_of_month_matches or day_of_week_matches

  def _matches_day_of_month(self, moment: datetime) -> bool:
    if self.day_of_month.wildcard:
      return True

    month_length = calendar.monthrange(moment.year, moment.month)[1]
    for day in self.day_of_month.values:
      if day > 0 and moment.day == day:
        return True
      if day < 0 and moment.day == month_length + 1 + day:
        return True
    return False


@dataclass(slots=True)
class _NaturalSlot:
  key: str
  data0: object
  data1: object = None
  weak: object = None
  strong: object = None

  def append(self, other: "_NaturalSlot") -> None:
    self.data0 = self._conflate(other, 0)
    self.data1 = self._conflate(other, 1)
    self.weak = None
    self.strong = None

  def values(self, index: int) -> list[object]:
    value = self.data0 if index == 0 else self.data1
    if value == "*":
      return ["*"]
    if value is None:
      return []
    if isinstance(value, (list, tuple, set)):
      return list(value)
    return [value]

  def _conflate(self, other: "_NaturalSlot", index: int) -> object:
    left = self.data0 if index == 0 else self.data1
    right = other.data0 if index == 0 else other.data1
    if right is None:
      return left
    if left is None:
      return right

    if index == 0 and other.strong == 1 and (hour_range := self._hour_range()):
      return _natural_hour_range(hour_range)
    if index == 0 and self.strong == 1 and (hour_range := other._hour_range()):
      return _natural_hour_range(hour_range)
    if self.strong == index or self.strong is True:
      return left
    if other.strong == index or other.strong is True:
      return right
    if other.weak == index or other.weak is True:
      return left
    if self.weak == index or self.weak is True:
      return right
    if left == "*" and right == "*":
      return "*"
    return [*_natural_data_values(left), *_natural_data_values(right)]

  def _hour_range(self) -> tuple[int, int] | None:
    if self.key != "hm" or self.data1 != 0:
      return None
    match = re.fullmatch(r"(\d+)-(\d+)", str(self.data0))
    if match is None:
      return None
    return int(match.group(1)), int(match.group(2))


def is_valid_cron(schedule: str) -> bool:
  try:
    _parse_schedule(str(schedule))
  except (TypeError, ValueError, ZoneInfoNotFoundError):
    return False
  return True


def next_cron_run(schedule: str, base: datetime) -> datetime:
  return _find_run(schedule, base, direction=1)


def previous_cron_run(schedule: str, base: datetime) -> datetime:
  return _find_run(schedule, base, direction=-1)


def latest_cron_run(schedule: str, base: datetime) -> datetime:
  return previous_cron_run(schedule, base + timedelta(microseconds=1))


@lru_cache(maxsize=512)
def _parse_schedule(schedule: str) -> tuple[_Schedule, ...]:
  if not isinstance(schedule, str):
    raise TypeError("cron schedule must be a string")

  schedule = schedule.strip()
  if schedule.startswith("@"):
    return (_parse_cron_schedule(_expand_special(schedule)),)

  try:
    return (_parse_cron_schedule(_expand_special(schedule)),)
  except (ValueError, ZoneInfoNotFoundError):
    return tuple(_parse_cron_schedule(cron) for cron in _expand_natural_schedule(schedule))


def _parse_cron_schedule(schedule: str) -> _Schedule:
  fields, schedule_timezone = _split_cron_schedule(schedule)

  if len(fields) == 5:
    seconds = _Field(frozenset({0}), wildcard=False)
    minute_text, hour_text, day_of_month_text, month_text, day_of_week_text = fields
  elif len(fields) == 6:
    second_text, minute_text, hour_text, day_of_month_text, month_text, day_of_week_text = fields
    seconds = _parse_simple_field(second_text, minimum=0, maximum=59)
  else:
    raise ValueError("cron schedules must have five or six fields")

  day_of_month_text, day_of_month_and = _strip_day_and(day_of_month_text)
  day_of_week_text, day_of_week_and = _strip_day_and(day_of_week_text)
  day_and = day_of_month_and or day_of_week_and

  parsed = _Schedule(
    seconds=seconds,
    minutes=_parse_simple_field(minute_text, minimum=0, maximum=59),
    hours=_parse_simple_field(hour_text, minimum=0, maximum=24, normalize=_normalize_hour),
    day_of_month=_parse_monthday_field(day_of_month_text),
    months=_parse_simple_field(month_text, minimum=1, maximum=12, names=_MONTH_NAMES),
    day_of_week=_parse_weekday_field(day_of_week_text),
    day_and=day_and,
    timezone=schedule_timezone,
  )
  _ensure_possible_day_of_month(parsed)
  return parsed


def _split_cron_schedule(schedule: str) -> tuple[tuple[str, ...], tzinfo | None]:
  fields = schedule.split()
  if len(fields) == 5:
    return tuple(fields), None
  if len(fields) == 6:
    schedule_timezone = _parse_timezone(fields[-1], required=False)
    if schedule_timezone is not None:
      return tuple(fields[:-1]), schedule_timezone
    return tuple(fields), None
  if len(fields) == 7:
    return tuple(fields[:-1]), _parse_timezone(fields[-1], required=True)
  raise ValueError("cron schedules must have five or six fields")


def _expand_special(schedule: str) -> str:
  if not schedule:
    raise ValueError("cron schedule cannot be empty")
  if not schedule.startswith("@"):
    return schedule

  special, *rest = schedule.split()
  expanded = _SPECIALS.get(special.lower())
  if expanded is None:
    raise ValueError(f"unsupported cron special {special!r}")
  return " ".join((expanded, *rest))


def _expand_natural_schedule(schedule: str) -> tuple[str, ...]:
  schedule = re.sub(r"\s+", " ", schedule.strip())
  if not schedule:
    raise ValueError("natural schedule cannot be empty")
  if len(schedule) > 256:
    raise ValueError("natural schedule is too long")
  return _natural_slots_to_crons(_parse_natural_slots(schedule))


def _parse_natural_slots(schedule: str) -> list[_NaturalSlot]:
  remaining = schedule
  slots: list[_NaturalSlot] = []

  while remaining:
    remaining = _strip_natural_separator(remaining)
    if not remaining:
      break

    lower = remaining.lower()
    if lower.startswith("every "):
      segment, remaining = _take_natural_segment(remaining[6:])
      slots.extend(_parse_every_segment(segment))
    elif lower.startswith("from "):
      segment, remaining = _take_natural_segment(remaining[5:])
      slots.extend(_parse_from_segment(segment))
    elif lower.startswith("at "):
      segment, remaining = _take_natural_segment(remaining[3:])
      slots.extend(_parse_at_segment(segment))
    elif lower.startswith("in "):
      segment, remaining = _take_natural_segment(remaining[3:])
      slots.append(_parse_timezone_slot(segment))
    elif lower.startswith("on "):
      segment, remaining = _take_natural_segment(remaining[3:])
      if _parse_timezone(segment, required=False) is not None:
        slots.append(_parse_timezone_slot(segment))
      else:
        slots.extend(_parse_on_segment(segment))
    else:
      segment, remaining = _take_natural_segment(remaining)
      slots.extend(_parse_bare_natural_segment(segment))

  if not slots:
    raise ValueError("natural schedule has no time information")
  return slots


def _strip_natural_separator(value: str) -> str:
  return value.strip(" ,\t")


def _take_natural_segment(value: str) -> tuple[str, str]:
  value = _strip_natural_separator(value)
  match = re.search(r"\s+(?=(?:every|from|at|on|in)\b)", value, re.IGNORECASE)
  if match is None:
    return value, ""
  return value[: match.start()].strip(), value[match.end() :]


def _parse_bare_natural_segment(segment: str) -> list[_NaturalSlot]:
  parsers = (_parse_at_segment, _parse_from_segment)
  for parser in parsers:
    try:
      return parser(segment)
    except ValueError:
      continue
  raise ValueError(f"invalid natural schedule segment {segment!r}")


def _parse_every_segment(segment: str) -> list[_NaturalSlot]:
  try:
    return _parse_every_subject(segment)
  except ValueError:
    pass

  tokens = segment.split()
  for index in range(1, len(tokens)):
    subject = " ".join(tokens[:index])
    time_text = " ".join(tokens[index:])
    try:
      return [*_parse_every_subject(subject), *_parse_at_segment(time_text)]
    except ValueError:
      continue
  raise ValueError(f"invalid natural every expression {segment!r}")


def _parse_every_subject(subject: str) -> list[_NaturalSlot]:
  normalized = subject.strip()
  lower = normalized.lower()
  if lower in {"weekday", "weekdays"}:
    return [_NaturalSlot("weekday", "1-5", weak=True)]

  interval_slot = _parse_every_interval_slot(normalized)
  if interval_slot is not None:
    return interval_slot
  if match := re.fullmatch(r"(.+)\s+of the month", lower):
    return [_NaturalSlot("monthday", _parse_monthday_list(match.group(1)))]
  return [_NaturalSlot("weekday", _parse_weekday_list(lower))]


def _parse_every_interval_slot(subject: str) -> list[_NaturalSlot] | None:
  match = re.fullmatch(r"(\d+)([A-Za-z]+)", subject) or re.fullmatch(
    r"(?:(\d+)\s+)?([A-Za-z]+)", subject
  )
  if match is None:
    return None

  count_text, unit_text = match.groups()
  unit = "month" if unit_text == "M" else _NATURAL_INTERVAL_UNITS.get(unit_text.lower())
  if unit is None:
    return None

  count = int(count_text or 1)
  if count <= 0:
    raise ValueError("natural interval count must be positive")
  step = "*" if count == 1 else f"*/{count}"
  if unit == "second":
    return [_NaturalSlot("second", step)]
  if unit == "minute":
    return [_NaturalSlot("hm", "*", step, strong=1)]
  if unit == "hour":
    return [_NaturalSlot("hm", step, 0, weak=1)]
  if unit == "day":
    return [_NaturalSlot("monthday", step, weak=True)]
  if unit == "month":
    return [_NaturalSlot("month", step)]
  if unit == "week":
    if count != 1:
      raise ValueError("natural week intervals only support every week")
    return [_NaturalSlot("weekday", 0, weak=True)]
  if count != 1:
    raise ValueError("natural year intervals only support every year")
  return [_NaturalSlot("month", 1, weak=True), _NaturalSlot("monthday", 1, weak=True)]


def _parse_on_segment(segment: str) -> list[_NaturalSlot]:
  try:
    return _parse_on_object(segment)
  except ValueError:
    pass

  tokens = segment.split()
  for index in range(1, len(tokens)):
    subject = " ".join(tokens[:index])
    time_text = " ".join(tokens[index:])
    try:
      return [*_parse_on_object(subject), *_parse_at_segment(time_text)]
    except ValueError:
      continue
  raise ValueError(f"invalid natural on expression {segment!r}")


def _parse_on_object(segment: str) -> list[_NaturalSlot]:
  lower = segment.strip().lower()
  if match := re.fullmatch(r"days?\s+(.+)", lower):
    return [_NaturalSlot("monthday", _parse_monthday_list(match.group(1)))]
  if match := re.fullmatch(r"minutes?\s+(.+)", lower):
    return [_NaturalSlot("hm", "*", _parse_number_list(match.group(1)), strong=1)]
  if match := re.fullmatch(r"seconds?\s+(.+)", lower):
    return [_NaturalSlot("second", _parse_number_list(match.group(1)))]
  if re.fullmatch(r"(?:the\s+)?hour", lower):
    return [_NaturalSlot("hm", "*", 0, strong=1)]
  if re.fullmatch(r"(?:the\s+)?minute", lower):
    return [_NaturalSlot("hm", "*", "*", strong=1)]
  if match := re.fullmatch(r"the\s+(.+)", lower):
    return [_NaturalSlot("monthday", _parse_monthday_list(match.group(1)))]
  return [_NaturalSlot("weekday", _parse_weekday_list(lower))]


def _parse_from_segment(segment: str) -> list[_NaturalSlot]:
  parsers = (_parse_weekday_range_slot, _parse_monthday_range_slot, _parse_hour_range_slot)
  for parser in parsers:
    try:
      return [parser(segment)]
    except ValueError:
      continue
  raise ValueError(f"invalid natural from expression {segment!r}")


def _parse_at_segment(segment: str) -> list[_NaturalSlot]:
  segment, timezone_text = _split_at_timezone(segment)
  lower = segment.strip().lower()
  slots: list[_NaturalSlot]
  if lower in {"the hour", "hour"}:
    slots = [_NaturalSlot("hm", "*", 0, strong=1)]
  elif lower in {"the minute", "minute"}:
    slots = [_NaturalSlot("hm", "*", "*", strong=1)]
  elif match := re.fullmatch(r"(minutes?|mins?)\s+(.+)", lower):
    slots = [_NaturalSlot("hm", "*", _parse_number_list(match.group(2)), strong=1)]
  elif match := re.fullmatch(r"(seconds?|secs?)\s+(.+)", lower):
    slots = [_NaturalSlot("second", _parse_number_list(match.group(2)))]
  elif match := re.fullmatch(r"(hours?|hou|h)\s+(.+)", lower):
    slots = [_NaturalSlot("hm", _parse_number_list(match.group(2)), 0)]
  else:
    slots = [_NaturalSlot("hm", hour, minute) for hour, minute in _parse_natural_times(segment)]

  if timezone_text is not None:
    slots.append(_NaturalSlot("tz", timezone_text))
  return slots


def _split_at_timezone(segment: str) -> tuple[str, str | None]:
  parts = segment.strip().split()
  if len(parts) < 2:
    return segment.strip(), None
  timezone_text = parts[-1]
  if _parse_timezone(timezone_text, required=False) is None:
    return segment.strip(), None
  return " ".join(parts[:-1]).strip(), timezone_text


def _parse_timezone_slot(segment: str) -> _NaturalSlot:
  timezone_text = segment.strip()
  if _parse_timezone(timezone_text, required=False) is None:
    raise ValueError(f"invalid timezone {timezone_text!r}")
  return _NaturalSlot("tz", timezone_text)


def _parse_natural_times(segment: str) -> list[tuple[int, int]]:
  return [_parse_natural_time(part) for part in _natural_time_parts(segment)]


def _parse_weekday_range_slot(segment: str) -> _NaturalSlot:
  start_text, end_text = _split_natural_range(segment)
  return _NaturalSlot(
    "weekday", f"{_natural_weekday_number(start_text)}-{_natural_weekday_number(end_text)}"
  )


def _parse_monthday_range_slot(segment: str) -> _NaturalSlot:
  start_text, end_text = _split_natural_range(segment)
  if _INT_RE.fullmatch(start_text) and _INT_RE.fullmatch(end_text):
    raise ValueError(f"invalid natural monthday range {segment!r}")
  return _NaturalSlot(
    "monthday", f"{_parse_monthday_value(start_text)}-{_parse_monthday_value(end_text)}"
  )


def _parse_hour_range_slot(segment: str) -> _NaturalSlot:
  start_text, end_text = _split_natural_range(segment)
  start_hour, start_minute = _parse_natural_time(start_text)
  end_hour, end_minute = _parse_natural_time(end_text)
  if start_minute != end_minute:
    raise ValueError("natural hour ranges must use the same minute value")
  return _NaturalSlot("hm", f"{start_hour}-{end_hour}", start_minute, strong=0)


def _split_natural_range(segment: str) -> tuple[str, str]:
  normalized = re.sub(r"\bthe\s+", "", segment.strip().lower())
  match = re.fullmatch(r"(.+?)\s*(?:-|\bto\b|\bthrough\b)\s*(.+)", normalized)
  if match is None:
    raise ValueError(f"invalid natural range {segment!r}")
  return match.group(1).strip(), match.group(2).strip()


def _parse_weekday_list(value: str) -> list[object]:
  return [_parse_weekday_part(part) for part in _natural_list_parts(value)]


def _parse_weekday_part(value: str) -> object:
  if re.search(r"(?:-|\bto\b|\bthrough\b)", value):
    start_text, end_text = _split_natural_range(value)
    return f"{_natural_weekday_number(start_text)}-{_natural_weekday_number(end_text)}"
  return _natural_weekday_number(value)


def _parse_monthday_list(value: str) -> list[object]:
  return [_parse_monthday_value(part) for part in _natural_list_parts(value)]


def _parse_monthday_value(value: str) -> object:
  normalized = re.sub(r"\bthe\s+", "", value.strip().lower())
  if _INT_RE.fullmatch(normalized):
    number = int(normalized)
    if 1 <= number <= 31:
      return number
  if normalized in _NATURAL_MONTHDAYS:
    return _NATURAL_MONTHDAYS[normalized]
  raise ValueError(f"invalid natural monthday {value!r}")


def _parse_number_list(value: str) -> list[int]:
  return [
    _parse_natural_number(part, minimum=0, maximum=59) for part in _natural_list_parts(value)
  ]


def _natural_list_parts(value: str) -> tuple[str, ...]:
  parts = tuple(part.strip() for part in _NATURAL_LIST_RE.split(value) if part.strip())
  if not parts:
    raise ValueError("natural list has no values")
  return parts


def _natural_slots_to_crons(slots: list[_NaturalSlot]) -> tuple[str, ...]:
  grouped_slots: dict[str, _NaturalSlot] = {}
  hms: list[_NaturalSlot] = []
  for slot in slots:
    if slot.key == "hm":
      hms.append(slot)
    elif slot.key in grouped_slots:
      grouped_slots[slot.key].append(slot)
    else:
      grouped_slots[slot.key] = slot

  if ("monthday" in grouped_slots or "weekday" in grouped_slots) and not hms:
    hms.append(_NaturalSlot("hm", 0, 0))
  elif "month" in grouped_slots:
    if not hms:
      hms.append(_NaturalSlot("hm", 0, 0))
    grouped_slots.setdefault("monthday", _NaturalSlot("monthday", 1))

  return tuple(_natural_cron_from_hm(grouped_slots, hm) for hm in _determine_natural_hms(hms))


def _determine_natural_hms(hms: list[_NaturalSlot]) -> list[_NaturalSlot]:
  if not hms:
    return [_NaturalSlot("hm", "*", "*")]

  hms = [*_hms_clone(hms)]
  while len(hms) > 1:
    graded_index = next((index for index, hm in enumerate(hms) if hm.weak or hm.strong), None)
    if graded_index is None:
      break
    graded = hms[graded_index]
    other_index = 1 if graded_index == 0 else graded_index - 1
    graded.append(hms.pop(other_index))

  grouped: dict[tuple[object, ...], set[object]] = {}
  for hm in hms:
    for minute in hm.values(1):
      key = tuple(sorted(set(hm.values(0)), key=_natural_sort_key))
      grouped.setdefault(key, set()).add(minute)
  return [
    _NaturalSlot("hm", list(hours), sorted(minutes, key=_natural_sort_key))
    for hours, minutes in grouped.items()
  ]


def _hms_clone(hms: list[_NaturalSlot]):
  for hm in hms:
    yield _NaturalSlot(hm.key, hm.data0, hm.data1, hm.weak, hm.strong)


def _natural_cron_from_hm(slots: dict[str, _NaturalSlot], hm: _NaturalSlot) -> str:
  fields: list[list[object]] = [
    _natural_slot_values(slots, "second", "0"),
    hm.values(1),
    hm.values(0),
    _natural_slot_values(slots, "monthday", "*"),
    _natural_slot_values(slots, "month", "*"),
    _natural_slot_values(slots, "weekday", "*"),
  ]
  if fields[0] == ["0"]:
    fields.pop(0)
  if timezone_slot := slots.get("tz"):
    fields.append(timezone_slot.values(0))
  return " ".join(_natural_field_text(field) for field in fields)


def _natural_slot_values(
  slots: dict[str, _NaturalSlot], key: str, default: object
) -> list[object]:
  slot = slots.get(key)
  if slot is None:
    return [default]
  return slot.values(0)


def _natural_field_text(values: list[object]) -> str:
  unique_values = sorted(set(values), key=_natural_sort_key)
  return ",".join(str(value) for value in unique_values)


def _natural_sort_key(value: object) -> tuple[int, object]:
  if isinstance(value, int):
    return 0, value
  if isinstance(value, str) and _INT_RE.fullmatch(value):
    return 0, int(value)
  return 1, str(value)


def _natural_data_values(value: object) -> list[object]:
  if value == "*":
    return []
  if isinstance(value, (list, tuple, set)):
    return list(value)
  return [value]


def _natural_hour_range(hour_range: tuple[int, int]) -> object:
  start, end = hour_range
  end -= 1
  return start if start == end else f"{start}-{end}"


def _natural_weekday_number(value: str) -> int:
  try:
    return _NATURAL_WEEKDAY_NAMES[value.strip().lower()]
  except KeyError as exc:
    raise ValueError(f"invalid natural weekday {value!r}") from exc


def _natural_time_parts(time_text: str) -> tuple[str, ...]:
  parts = tuple(part.strip() for part in _NATURAL_LIST_RE.split(time_text) if part.strip())
  if not parts:
    raise ValueError("natural schedule has no time")
  return parts


def _parse_natural_time(time_text: str) -> tuple[int, int]:
  normalized = re.sub(r"\s+", " ", time_text.strip().lower())
  normalized = re.sub(r"\s+o'clock\Z", "", normalized)
  if normalized in {"noon", "midday"}:
    return 12, 0
  if normalized == "midnight":
    return 0, 0

  match = re.fullmatch(r"(.+?)(?:\s*(am|pm|noon|midday|midnight))?", normalized)
  if match is None:
    raise ValueError(f"invalid natural time {time_text!r}")

  clock_text, meridiem = match.groups()
  if ":" in clock_text:
    hour_text, minute_text = clock_text.split(":", 1)
    hour = _parse_natural_number(hour_text, minimum=0, maximum=24)
    minute = _parse_natural_number(minute_text, minimum=0, maximum=59)
  else:
    parts = clock_text.split()
    if len(parts) > 2:
      raise ValueError(f"invalid natural time {time_text!r}")
    hour = _parse_natural_number(parts[0], minimum=0, maximum=24)
    minute = _parse_natural_number(parts[1], minimum=0, maximum=59) if len(parts) == 2 else 0

  return _adjust_natural_hour(hour, meridiem), minute


def _adjust_natural_hour(hour: int, meridiem: str | None) -> int:
  if meridiem in {"pm", "noon", "midday"} and hour < 12:
    return hour + 12
  if meridiem == "am" and hour == 12:
    return 0
  if meridiem == "midnight" and hour == 12:
    return 0
  return hour


def _parse_natural_number(value: str | None, *, minimum: int, maximum: int) -> int:
  if value is None:
    raise ValueError("natural number is required")
  normalized = value.strip().lower()
  if _INT_RE.fullmatch(normalized):
    number = int(normalized)
  else:
    number = _parse_natural_number_word(normalized)
  if number < minimum:
    raise ValueError(f"natural number {number!r} is outside {minimum}-{maximum}")
  if number > maximum:
    raise ValueError(f"natural number {number!r} is outside {minimum}-{maximum}")
  return number


def _parse_natural_number_word(value: str) -> int:
  if value in _NATURAL_SMALL_NUMBERS:
    return _NATURAL_SMALL_NUMBERS[value]
  if value in _NATURAL_TENS:
    return _NATURAL_TENS[value]

  parts = re.split(r"[- ]", value)
  if len(parts) == 2 and parts[0] in _NATURAL_TENS and parts[1] in _NATURAL_SMALL_NUMBERS:
    return _NATURAL_TENS[parts[0]] + _NATURAL_SMALL_NUMBERS[parts[1]]
  raise ValueError(f"invalid natural number {value!r}")


def _parse_timezone(value: str, *, required: bool) -> tzinfo | None:
  if value == "Z":
    return UTC

  match = _OFFSET_RE.fullmatch(value)
  if match is not None:
    sign, hour_text, minute_text = match.groups()
    hours = int(hour_text)
    minutes = int(minute_text or 0)
    if hours > 23 or minutes > 59:
      raise ValueError(f"invalid timezone offset {value!r}")
    offset = timedelta(hours=hours, minutes=minutes)
    if sign == "-":
      offset = -offset
    return timezone(offset, value)

  try:
    return ZoneInfo(value)
  except ZoneInfoNotFoundError:
    if required:
      raise ValueError(f"invalid timezone {value!r}") from None
    return None


def _strip_day_and(expression: str) -> tuple[str, bool]:
  if not expression.endswith("&"):
    return expression, False
  expression = expression[:-1]
  if not expression:
    raise ValueError("cron day fields cannot contain only &")
  return expression, True


def _parse_simple_field(
  expression: str,
  *,
  minimum: int,
  maximum: int,
  names: dict[str, int] | None = None,
  normalize=None,
  single_step_to_max: bool = True,
  full_range_is_wildcard: bool = True,
) -> _Field:
  if normalize is None:
    normalize = _identity

  items = _field_items(expression)
  values: set[int] = set()
  wildcard = False
  all_values = frozenset(normalize(value) for value in range(minimum, maximum + 1))

  for item in items:
    item_values, item_wildcard = _expand_simple_item(
      item,
      minimum=minimum,
      maximum=maximum,
      names=names,
      normalize=normalize,
      single_step_to_max=single_step_to_max,
      full_range_is_wildcard=full_range_is_wildcard,
      all_values=all_values,
    )
    values.update(item_values)
    wildcard = wildcard or item_wildcard

  if wildcard:
    values = set(all_values)
  if not values:
    raise ValueError("cron field has no values")
  return _Field(values=frozenset(values), wildcard=wildcard)


def _expand_simple_item(
  item: str,
  *,
  minimum: int,
  maximum: int,
  names: dict[str, int] | None,
  normalize,
  single_step_to_max: bool,
  full_range_is_wildcard: bool,
  all_values: frozenset[int],
) -> tuple[set[int], bool]:
  range_text, step, stepped = _split_step(item)
  if range_text == "*":
    start, end, full_range = minimum, maximum, True
  else:
    start, end, has_range = _parse_simple_range(
      range_text, minimum=minimum, maximum=maximum, names=names
    )
    if stepped and not has_range and single_step_to_max:
      end = maximum
    full_range = full_range_is_wildcard and start == minimum and end == maximum

  values = set(_cyclic_values(start, end, minimum, maximum, step, normalize))
  wildcard = step == 1 and full_range and values == all_values
  return values, wildcard


def _parse_simple_range(
  range_text: str,
  *,
  minimum: int,
  maximum: int,
  names: dict[str, int] | None,
) -> tuple[int, int, bool]:
  if "-" not in range_text:
    value = _parse_simple_bound(range_text, minimum=minimum, maximum=maximum, names=names)
    return value, value, False

  start_text, end_text = range_text.split("-", 1)
  if not start_text or not end_text:
    raise ValueError("cron ranges require a start and end")
  return (
    _parse_simple_bound(start_text, minimum=minimum, maximum=maximum, names=names),
    _parse_simple_bound(end_text, minimum=minimum, maximum=maximum, names=names),
    True,
  )


def _parse_simple_bound(
  value: str,
  *,
  minimum: int,
  maximum: int,
  names: dict[str, int] | None,
) -> int:
  normalized = value.upper()
  if names is not None and normalized in names:
    number = names[normalized]
  elif _INT_RE.fullmatch(value):
    number = int(value)
  else:
    raise ValueError(f"invalid cron value {value!r}")

  if number < minimum or number > maximum:
    raise ValueError(f"cron value {number!r} is outside {minimum}-{maximum}")
  return number


def _parse_monthday_field(expression: str) -> _Field:
  values: set[int] = set()
  wildcard = False
  all_values = frozenset(range(1, 32))

  for item in _field_items(expression):
    item_values, item_wildcard = _expand_monthday_item(item)
    values.update(item_values)
    wildcard = wildcard or item_wildcard

  if wildcard:
    values = set(all_values)
  if not values:
    raise ValueError("cron day-of-month field has no values")
  return _Field(values=frozenset(values), wildcard=wildcard)


def _expand_monthday_item(item: str) -> tuple[set[int], bool]:
  range_text, step, stepped = _split_step(item)
  if range_text == "*":
    values = set(range(1, 32, step))
    return values, step == 1

  start, end, has_range = _parse_monthday_range(range_text)
  if stepped and not has_range:
    end = 31 if start > 0 else -1

  if start > 0:
    end = 31 if end < 0 else end
    values = set(_cyclic_values(start, end, 1, 31, step, _identity))
    return values, step == 1 and start == 1 and end == 31

  if end > 0:
    raise ValueError("negative day-of-month ranges must end with a negative value")
  values = set(_cyclic_values(start, end, -31, -1, step, _identity))
  return values, False


def _parse_monthday_range(range_text: str) -> tuple[int, int, bool]:
  match = re.fullmatch(r"(last|l|-?\d+)(?:-(last|l|-?\d+))?", range_text, re.IGNORECASE)
  if match is None:
    raise ValueError(f"invalid day-of-month value {range_text!r}")

  start_text, end_text = match.groups()
  start = _parse_monthday_bound(start_text)
  if end_text is None:
    return start, start, False
  return start, _parse_monthday_bound(end_text), True


def _parse_monthday_bound(value: str) -> int:
  if value.lower() in {"l", "last"}:
    return -1
  if not _INT_RE.fullmatch(value):
    raise ValueError(f"invalid day-of-month value {value!r}")

  number = int(value)
  if number == 0 or number < -31 or number > 31:
    raise ValueError(f"day-of-month value {number!r} is outside 1-31 or -31--1")
  return number


def _parse_weekday_field(expression: str) -> _WeekdayField:
  specs: set[_WeekdaySpec] = set()
  wildcard = False

  for item in _field_items(expression):
    if "#" in item:
      specs.add(_parse_weekday_hash(item))
      continue
    if "%" in item:
      specs.add(_parse_weekday_modulo(item))
      continue

    field = _parse_simple_field(
      item,
      minimum=0,
      maximum=7,
      names=_WEEKDAY_NAMES,
      normalize=_normalize_day_of_week,
      single_step_to_max=False,
      full_range_is_wildcard=False,
    )
    if field.wildcard:
      wildcard = True
    else:
      specs.update(_WeekdaySpec(day=value) for value in field.values)

  if wildcard:
    return _WeekdayField(specs=(), wildcard=True)
  if not specs:
    raise ValueError("cron day-of-week field has no values")
  return _WeekdayField(specs=tuple(sorted(specs, key=_weekday_spec_sort_key)), wildcard=False)


def _parse_weekday_hash(item: str) -> _WeekdaySpec:
  if item.count("#") != 1:
    raise ValueError("weekday hash expressions can contain one #")
  day_text, nth_text = item.split("#", 1)
  day = _parse_weekday_bound(day_text)
  nth = -1 if nth_text.lower() in {"l", "last"} else _parse_hash_index(nth_text)
  return _WeekdaySpec(day=day, nth=nth)


def _parse_weekday_modulo(item: str) -> _WeekdaySpec:
  if item.count("%") != 1:
    raise ValueError("weekday modulo expressions can contain one %")
  day_text, modulo_text = item.split("%", 1)
  match = _WEEKDAY_MODULO_RE.fullmatch(modulo_text)
  if match is None:
    raise ValueError("weekday modulo expressions must use %n or %n+offset")

  modulo = int(match.group(1))
  offset = int(match.group(2) or 0)
  if modulo <= 0:
    raise ValueError("weekday modulo must be positive")
  return _WeekdaySpec(day=_parse_weekday_bound(day_text), modulo=(modulo, offset))


def _parse_weekday_bound(value: str) -> int:
  return _normalize_day_of_week(
    _parse_simple_bound(value, minimum=0, maximum=7, names=_WEEKDAY_NAMES)
  )


def _parse_hash_index(value: str) -> int:
  if not _INT_RE.fullmatch(value):
    raise ValueError("weekday hash index must be an integer")
  number = int(value)
  if number == 0 or number < -5 or number > 5:
    raise ValueError("weekday hash index must be between 1 and 5 or -1 and -5")
  return number


def _field_items(expression: str) -> tuple[str, ...]:
  items = tuple(item.strip() for item in expression.split(",") if item.strip())
  if not items:
    raise ValueError("empty cron field")
  return items


def _split_step(item: str) -> tuple[str, int, bool]:
  if item.startswith("/"):
    return "*", _parse_step(item[1:]), True
  if "/" not in item:
    return item, 1, False
  if item.count("/") != 1:
    raise ValueError("cron field items can contain one step")

  range_text, step_text = item.split("/", 1)
  if not range_text:
    range_text = "*"
  return range_text, _parse_step(step_text), True


def _parse_step(value: str) -> int:
  if not _INT_RE.fullmatch(value):
    raise ValueError("cron steps must be integers")
  step = int(value)
  if step <= 0:
    raise ValueError("cron steps must be positive")
  return step


def _cyclic_values(
  start: int, end: int, minimum: int, maximum: int, step: int, normalize
) -> list[int]:
  if start <= end:
    raw_values = list(range(start, end + 1))
  else:
    raw_values = [*range(start, maximum + 1), *range(minimum, end + 1)]
  return [normalize(value) for index, value in enumerate(raw_values) if index % step == 0]


def _find_run(schedule: str, base: datetime, *, direction: int) -> datetime:
  if not isinstance(base, datetime):
    raise TypeError("base must be a datetime")

  candidates = []
  for parsed in _parse_schedule(str(schedule)):
    try:
      candidates.append(_find_run_for_schedule(parsed, base, direction=direction))
    except ValueError:
      continue
  if not candidates:
    raise ValueError("cron schedule has no matching datetime in the search window")
  if direction > 0:
    return min(candidates)
  return max(candidates)


def _find_run_for_schedule(schedule: _Schedule, base: datetime, *, direction: int) -> datetime:
  timeline_base, local_timezone, return_timezone, return_naive = _timeline_base(schedule, base)
  local_base = _to_local_datetime(timeline_base, local_timezone)
  local_date = local_base.date()
  date_step = _ONE_DAY if direction > 0 else -_ONE_DAY

  for _ in range(_MAX_SEARCH_DAYS):
    if _matches_date(schedule, local_date, local_timezone):
      for candidate in _candidate_times(
        schedule,
        local_date,
        local_base,
        local_timezone,
        direction,
      ):
        if (direction > 0 and candidate > timeline_base) or (
          direction < 0 and candidate < timeline_base
        ):
          return _restore_timeline(candidate, return_timezone, local_timezone, return_naive)
    local_date += date_step

  raise ValueError("cron schedule has no matching datetime in the search window")


def _timeline_base(
  schedule: _Schedule, base: datetime
) -> tuple[datetime, tzinfo | None, tzinfo | None, bool]:
  if base.tzinfo is not None:
    return base.astimezone(UTC), schedule.timezone or base.tzinfo, base.tzinfo, False
  if schedule.timezone is not None:
    return base.replace(tzinfo=schedule.timezone).astimezone(UTC), schedule.timezone, None, True
  return base, None, None, True


def _to_local_datetime(moment: datetime, local_timezone: tzinfo | None) -> datetime:
  if local_timezone is None or moment.tzinfo is None:
    return moment
  return moment.astimezone(local_timezone)


def _restore_timeline(
  candidate: datetime,
  return_timezone: tzinfo | None,
  local_timezone: tzinfo | None,
  return_naive: bool,
) -> datetime:
  if return_naive:
    if candidate.tzinfo is not None and local_timezone is not None:
      return candidate.astimezone(local_timezone).replace(tzinfo=None)
    return candidate
  if return_timezone is None:
    return candidate
  return candidate.astimezone(return_timezone)


def _matches_date(schedule: _Schedule, local_date: date, local_timezone: tzinfo | None) -> bool:
  moment = datetime(local_date.year, local_date.month, local_date.day, tzinfo=local_timezone)
  return schedule.months.matches(moment.month) and schedule._matches_day(moment)


def _candidate_times(
  schedule: _Schedule,
  local_date: date,
  local_base: datetime,
  local_timezone: tzinfo | None,
  direction: int,
):
  same_date = local_date == local_base.date()
  skip_by_wall_time = local_timezone is None or not isinstance(local_timezone, ZoneInfo)
  for hour in _ordered_values(schedule.hours, direction):
    if same_date and skip_by_wall_time and direction > 0 and hour < local_base.hour:
      continue
    if same_date and skip_by_wall_time and direction < 0 and hour > local_base.hour:
      continue
    for minute in _ordered_values(schedule.minutes, direction):
      if same_date and skip_by_wall_time and hour == local_base.hour:
        if direction > 0 and minute < local_base.minute:
          continue
        if direction < 0 and minute > local_base.minute:
          continue
      for second in _ordered_values(schedule.seconds, direction):
        local_candidate = datetime(
          local_date.year,
          local_date.month,
          local_date.day,
          hour,
          minute,
          second,
          tzinfo=local_timezone,
        )
        yield from _timeline_candidates(local_candidate, local_timezone, direction)


def _timeline_candidates(
  local_candidate: datetime, local_timezone: tzinfo | None, direction: int
) -> tuple[datetime, ...]:
  if local_timezone is None:
    return (local_candidate,)

  candidates = set()
  for fold in (0, 1):
    folded = local_candidate.replace(fold=fold)
    timeline_candidate = folded.astimezone(UTC)
    if _same_local_time(timeline_candidate.astimezone(local_timezone), folded):
      candidates.add(timeline_candidate)
  return tuple(sorted(candidates, reverse=direction < 0))


def _same_local_time(left: datetime, right: datetime) -> bool:
  return (
    left.year,
    left.month,
    left.day,
    left.hour,
    left.minute,
    left.second,
  ) == (
    right.year,
    right.month,
    right.day,
    right.hour,
    right.minute,
    right.second,
  )


def _ordered_values(field: _Field, direction: int) -> list[int]:
  return sorted(field.values, reverse=direction < 0)


def _ensure_possible_day_of_month(schedule: _Schedule) -> None:
  if schedule.day_of_month.wildcard:
    return
  if not schedule.day_and and not schedule.day_of_week.wildcard:
    return

  months = schedule.months.values
  if any(
    _day_of_month_can_match(day, month) for month in months for day in schedule.day_of_month.values
  ):
    return
  raise ValueError("cron day of month is impossible for the selected months")


def _day_of_month_can_match(day: int, month: int) -> bool:
  month_length = calendar.monthrange(2024, month)[1]
  if day > 0:
    return day <= month_length
  return 1 <= month_length + 1 + day <= month_length


def _weekday_hash_matches(moment: datetime, nth: int) -> bool:
  month_length = calendar.monthrange(moment.year, moment.month)[1]
  from_start = ((moment.day - 1) // 7) + 1
  from_end = -(((month_length - moment.day) // 7) + 1)
  return nth == from_start or nth == from_end


def _weekday_modulo_matches(moment: datetime, modulo: tuple[int, int]) -> bool:
  divisor, offset = modulo
  relative_week = (moment.date() - _REFERENCE_MONDAY).days // 7
  return relative_week % divisor == offset % divisor


def _cron_weekday(moment: datetime) -> int:
  return (moment.weekday() + 1) % 7


def _weekday_spec_sort_key(spec: _WeekdaySpec) -> tuple[int, int, int, int]:
  if spec.modulo is None:
    modulo, offset = 0, 0
  else:
    modulo, offset = spec.modulo
  return spec.day, spec.nth or 0, modulo, offset


def _normalize_hour(value: int) -> int:
  if value == 24:
    return 0
  return value


def _normalize_day_of_week(value: int) -> int:
  if value == 7:
    return 0
  return value


def _identity(value: int) -> int:
  return value
