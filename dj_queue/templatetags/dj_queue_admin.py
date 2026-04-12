from django import template
from django.core.exceptions import FieldDoesNotExist
from django.db import models
from django.utils import timezone

register = template.Library()


def _format_datetime(value):
  if value is None:
    return None
  return timezone.template_localtime(value).strftime("%Y-%m-%d %H:%M:%S")


@register.filter
def djq_datetime(value):
  return _format_datetime(value)


@register.filter
def djq_admin_readonly_value(field):
  raw_field = getattr(field, "field", None)
  if not isinstance(raw_field, dict):
    return field.contents()

  field_name = raw_field.get("field")
  if not isinstance(field_name, str):
    return field.contents()

  instance = getattr(getattr(field, "form", None), "instance", None)
  if instance is None:
    return field.contents()

  try:
    model_field = instance._meta.get_field(field_name)
  except FieldDoesNotExist:
    return field.contents()

  if not isinstance(model_field, models.DateTimeField):
    return field.contents()

  value = getattr(instance, field_name)
  if value is None:
    return field.contents()
  return _format_datetime(value)
