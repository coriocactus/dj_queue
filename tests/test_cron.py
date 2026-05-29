from datetime import UTC, datetime

import pytest

from dj_queue.cron import is_valid_cron, latest_cron_run, next_cron_run, previous_cron_run


def dt(year, month, day, hour, minute, second=0):
  return datetime(year, month, day, hour, minute, second, tzinfo=UTC)


def test_every_minute_cron_is_exclusive_for_next_and_previous():
  base = dt(2026, 4, 8, 12, 0, 1)

  assert next_cron_run("* * * * *", base) == dt(2026, 4, 8, 12, 1)
  assert previous_cron_run("* * * * *", base) == dt(2026, 4, 8, 12, 0)


def test_latest_cron_run_includes_the_base_minute():
  assert latest_cron_run("* * * * *", dt(2026, 4, 8, 12, 0)) == dt(2026, 4, 8, 12, 0)


def test_cron_schedules_must_be_strings():
  assert is_valid_cron(1) is False
  with pytest.raises(TypeError, match="cron schedule must be a string"):
    next_cron_run(1, dt(2026, 4, 8, 12, 0))


def test_cron_steps_ranges_and_names():
  schedule = "*/15 9-17 * jan mon-fri"

  assert next_cron_run(schedule, dt(2026, 1, 5, 8, 59, 45)) == dt(2026, 1, 5, 9, 0)
  assert next_cron_run(schedule, dt(2026, 1, 5, 9, 0)) == dt(2026, 1, 5, 9, 15)


def test_cron_day_of_month_and_weekday_use_or_semantics():
  schedule = "0 0 1 * MON"

  assert next_cron_run(schedule, dt(2026, 3, 31, 23, 59)) == dt(2026, 4, 1, 0, 0)
  assert previous_cron_run(schedule, dt(2026, 4, 7, 0, 1)) == dt(2026, 4, 6, 0, 0)


def test_cron_day_of_month_impossibility_does_not_reject_or_weekday():
  schedule = "0 0 31 2 MON"

  assert is_valid_cron(schedule) is True
  assert next_cron_run(schedule, dt(2026, 2, 1, 0, 0)) == dt(2026, 2, 2, 0, 0)


def test_cron_treats_seven_as_sunday():
  assert next_cron_run("0 0 * * 7", dt(2026, 4, 4, 23, 59)) == dt(2026, 4, 5, 0, 0)


def test_cron_presets_expand_to_standard_schedules():
  assert next_cron_run("@daily", dt(2026, 4, 8, 12, 0)) == dt(2026, 4, 9, 0, 0)
  assert next_cron_run("@hourly", dt(2026, 4, 8, 12, 0)) == dt(2026, 4, 8, 13, 0)
  assert next_cron_run("@noon", dt(2026, 4, 8, 12, 0)) == dt(2026, 4, 9, 12, 0)


def test_cron_supports_seconds_field():
  schedule = "15,30 5 0 * * *"

  assert next_cron_run(schedule, dt(2026, 4, 8, 0, 5, 15)) == dt(2026, 4, 8, 0, 5, 30)
  assert previous_cron_run(schedule, dt(2026, 4, 8, 0, 5, 30)) == dt(2026, 4, 8, 0, 5, 15)
  assert next_cron_run("* * * * * *", dt(2026, 4, 8, 12, 0, 0)) == dt(2026, 4, 8, 12, 0, 1)


def test_cron_supports_last_and_negative_monthdays():
  assert next_cron_run("0 0 L * *", dt(2026, 2, 1, 0, 0)) == dt(2026, 2, 28, 0, 0)
  assert next_cron_run("0 0 -2 * *", dt(2026, 1, 1, 0, 0)) == dt(2026, 1, 30, 0, 0)
  assert next_cron_run("0 0 -7-L * *", dt(2026, 1, 24, 0, 0)) == dt(2026, 1, 25, 0, 0)


def test_cron_supports_nth_and_last_weekday_hashes():
  assert next_cron_run("0 5 * * mon#1", dt(2026, 4, 1, 0, 0)) == dt(2026, 4, 6, 5, 0)
  assert next_cron_run("0 7 * * fri#last", dt(2026, 1, 1, 0, 0)) == dt(2026, 1, 30, 7, 0)


def test_cron_supports_weekday_modulo():
  schedule = "0 12 * * mon%2,wed%3+1"

  assert next_cron_run(schedule, dt(2025, 9, 20, 12, 0)) == dt(2025, 9, 29, 12, 0)
  assert next_cron_run(schedule, dt(2025, 9, 29, 12, 0)) == dt(2025, 10, 1, 12, 0)


def test_cron_supports_day_of_month_and_weekday_and_extension():
  assert next_cron_run("0 0 */2 * 1-5", dt(2022, 8, 9, 0, 0)) == dt(2022, 8, 10, 0, 0)
  assert next_cron_run("0 0 */2 * 1-5&", dt(2022, 8, 9, 0, 0)) == dt(2022, 8, 11, 0, 0)


def test_cron_finds_sparse_day_and_schedule_beyond_eight_years():
  assert next_cron_run("0 0 29 2 MON&", dt(2024, 3, 1, 0, 0)) == dt(2044, 2, 29, 0, 0)
  assert previous_cron_run("0 0 29 2 MON&", dt(2045, 1, 1, 0, 0)) == dt(2044, 2, 29, 0, 0)


def test_cron_supports_wraparound_ranges_and_loose_commas():
  assert next_cron_run("55-5 7 * * *", dt(2026, 4, 8, 7, 59)) == dt(2026, 4, 9, 7, 0)
  assert next_cron_run("0 23 * * fri-sun", dt(2026, 4, 9, 23, 0)) == dt(2026, 4, 10, 23, 0)
  assert next_cron_run("9,,19 * * * *", dt(2026, 4, 8, 12, 10)) == dt(2026, 4, 8, 12, 19)


def test_cron_supports_leading_slashes_and_hour_twenty_four():
  assert next_cron_run("/15 * * * *", dt(2026, 4, 8, 12, 0)) == dt(2026, 4, 8, 12, 15)
  assert next_cron_run("0 24 * * *", dt(2026, 4, 8, 12, 0)) == dt(2026, 4, 9, 0, 0)


def test_cron_supports_explicit_timezones():
  assert next_cron_run("0 0 * * * Europe/Rome", dt(2026, 4, 8, 21, 59)) == dt(2026, 4, 8, 22, 0)


def test_cron_supports_natural_daily_schedule():
  assert is_valid_cron("every day at noon") is True
  assert next_cron_run("every day", dt(2026, 4, 8, 0, 0)) == dt(2026, 4, 9, 0, 0)
  assert next_cron_run("every day at noon", dt(2026, 4, 8, 11, 59)) == dt(2026, 4, 8, 12, 0)
  assert latest_cron_run("every day at noon", dt(2026, 4, 8, 12, 0)) == dt(2026, 4, 8, 12, 0)
  assert next_cron_run("every day 16:30", dt(2026, 4, 8, 16, 0)) == dt(2026, 4, 8, 16, 30)
  assert next_cron_run("every day at 16:00 and 18:00", dt(2026, 4, 8, 16, 0)) == dt(
    2026, 4, 8, 18, 0
  )
  assert next_cron_run("every day at 16:15 and 18:30", dt(2026, 4, 8, 16, 15)) == dt(
    2026, 4, 8, 18, 30
  )
  assert previous_cron_run("every day at 16:15 and 18:30", dt(2026, 4, 8, 18, 30)) == dt(
    2026, 4, 8, 16, 15
  )


def test_cron_supports_natural_weekday_schedule():
  assert next_cron_run("every weekday at five", dt(2026, 4, 10, 5, 0)) == dt(2026, 4, 13, 5, 0)
  assert next_cron_run("every tuesday and monday at 5pm and 11", dt(2026, 4, 13, 10, 0)) == dt(
    2026, 4, 13, 11, 0
  )


def test_cron_supports_natural_interval_schedule():
  assert next_cron_run("every 5 minutes", dt(2026, 4, 8, 12, 2)) == dt(2026, 4, 8, 12, 5)
  assert next_cron_run("every 15s", dt(2026, 4, 8, 12, 0, 14)) == dt(2026, 4, 8, 12, 0, 15)
  assert next_cron_run("every 3 hours", dt(2026, 4, 8, 3, 0)) == dt(2026, 4, 8, 6, 0)
  assert next_cron_run("every 4 months", dt(2026, 1, 1, 0, 0)) == dt(2026, 5, 1, 0, 0)
  assert next_cron_run("every 4 M", dt(2026, 1, 1, 0, 0)) == dt(2026, 5, 1, 0, 0)
  assert next_cron_run("every week", dt(2026, 4, 5, 0, 0)) == dt(2026, 4, 12, 0, 0)
  assert next_cron_run("every year", dt(2026, 1, 1, 0, 0)) == dt(2027, 1, 1, 0, 0)


def test_cron_supports_natural_timezone_schedule():
  assert next_cron_run("every day at midnight in Europe/Rome", dt(2026, 4, 8, 21, 59)) == dt(
    2026, 4, 8, 22, 0
  )
  assert next_cron_run("every day at 18:00 UTC", dt(2026, 4, 8, 17, 59)) == dt(2026, 4, 8, 18, 0)
  assert next_cron_run("every day at 6 pm in Asia/Tokyo", dt(2026, 4, 8, 8, 59)) == dt(
    2026, 4, 8, 9, 0
  )


def test_cron_supports_natural_on_clauses():
  assert next_cron_run("every month on day 2 at 10:00", dt(2026, 1, 1, 0, 0)) == dt(
    2026, 1, 2, 10, 0
  )
  assert next_cron_run("every month on days 1,15 at 10:00", dt(2026, 1, 2, 0, 0)) == dt(
    2026, 1, 15, 10, 0
  )
  assert next_cron_run("every week on monday 18:23", dt(2026, 4, 8, 0, 0)) == dt(
    2026, 4, 13, 18, 23
  )
  assert next_cron_run("every month on the 1st", dt(2026, 1, 1, 0, 0)) == dt(2026, 2, 1, 0, 0)


def test_cron_supports_natural_ordinals_and_from_clauses():
  assert next_cron_run("every first of the month", dt(2026, 1, 1, 0, 0)) == dt(2026, 2, 1, 0, 0)
  assert next_cron_run("every last of the month", dt(2026, 1, 30, 0, 0)) == dt(2026, 1, 31, 0, 0)
  assert next_cron_run("from monday to friday at 9", dt(2026, 4, 10, 9, 0)) == dt(
    2026, 4, 13, 9, 0
  )
  assert next_cron_run("from the 1st to the 3rd at noon", dt(2026, 1, 1, 12, 0)) == dt(
    2026, 1, 2, 12, 0
  )
  assert next_cron_run("from 9 to 17 at minute 10", dt(2026, 4, 8, 9, 10)) == dt(
    2026, 4, 8, 10, 10
  )


def test_cron_supports_natural_point_schedules():
  assert next_cron_run("at minute 5", dt(2026, 4, 8, 12, 4)) == dt(2026, 4, 8, 12, 5)
  assert next_cron_run("at seconds 10,20", dt(2026, 4, 8, 12, 0, 10)) == dt(2026, 4, 8, 12, 0, 20)
  assert next_cron_run("every day at the hour", dt(2026, 4, 8, 12, 0)) == dt(2026, 4, 8, 13, 0)


@pytest.mark.parametrize(
  "schedule",
  (
    "not a cron",
    "* * * *",
    "60 * * * *",
    "*/0 * * * *",
    "0 0 31 2 *",
    "@reboot",
    "* * * * * Missing/Zone",
    "0 0 * * mon#6",
    "0 0 * * sun%0",
    "every 2 weeks",
    "every day at 25:00",
  ),
)
def test_invalid_cron_schedules_are_rejected(schedule):
  assert is_valid_cron(schedule) is False
