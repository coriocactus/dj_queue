from dj_queue.db import get_database_alias


class DjQueueRouter:
  app_label = "dj_queue"

  def db_for_read(self, model, **hints):
    if model._meta.app_label == self.app_label:
      return get_database_alias()
    return None

  def db_for_write(self, model, **hints):
    if model._meta.app_label == self.app_label:
      return get_database_alias()
    return None

  def allow_relation(self, obj1, obj2, **hints):
    labels = {obj1._meta.app_label, obj2._meta.app_label}
    if self.app_label in labels:
      return labels == {self.app_label}
    return None

  def allow_migrate(self, db, app_label, **hints):
    if app_label == self.app_label:
      return db == get_database_alias()
    return None
