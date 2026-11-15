from dj_queue.config import configured_backend_aliases, load_backend_config


class DjQueueRouter:
  app_label = "dj_queue"

  def db_for_read(self, model, **hints):
    if model._meta.app_label == self.app_label:
      return self._single_database_alias()
    return None

  def db_for_write(self, model, **hints):
    if model._meta.app_label == self.app_label:
      return self._single_database_alias()
    return None

  def allow_relation(self, obj1, obj2, **hints):
    labels = {obj1._meta.app_label, obj2._meta.app_label}
    if self.app_label in labels:
      return labels == {self.app_label}
    return None

  def allow_migrate(self, db, app_label, **hints):
    if app_label == self.app_label:
      return db in self._database_aliases()
    return None

  def _single_database_alias(self):
    aliases = self._database_aliases()
    if len(aliases) == 1:
      return aliases.pop()
    return None

  def _database_aliases(self):
    return {load_backend_config(alias).database_alias for alias in configured_backend_aliases()}
