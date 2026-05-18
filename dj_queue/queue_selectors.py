from django.db.models import Q


def selectors_match_all(selectors):
  return selectors in (None, (), "*", ["*"], ("*",))


def normalize_queue_selectors(selectors):
  if selectors is None:
    return ()
  if isinstance(selectors, str):
    return (selectors,)
  return tuple(selectors)


def queue_matches_selectors(queue_name, selectors):
  if selectors_match_all(selectors):
    return True
  return any(_queue_matches_selector(queue_name, selector) for selector in normalize_queue_selectors(selectors))


def any_queue_matches_selectors(queue_names, selectors):
  if queue_names is None:
    return True
  if selectors_match_all(selectors):
    return True
  return any(queue_matches_selectors(queue_name, selectors) for queue_name in queue_names)


def filter_by_queue_selectors(queryset, selectors, *, field_name="queue_name"):
  condition = queue_selector_condition(selectors, field_name=field_name)
  if condition is None:
    return queryset
  if not condition:
    return queryset.none()
  return queryset.filter(condition)


def queue_selector_condition(selectors, *, field_name="queue_name"):
  if selectors_match_all(selectors):
    return None

  condition = Q()
  for selector in normalize_queue_selectors(selectors):
    if selector == "*":
      return None
    if selector.endswith("*"):
      condition |= Q(**{f"{field_name}__startswith": selector[:-1]})
    else:
      condition |= Q(**{field_name: selector})
  return condition


def _queue_matches_selector(queue_name, selector):
  if selector == "*":
    return True
  if selector.endswith("*"):
    return queue_name.startswith(selector[:-1])
  return queue_name == selector
