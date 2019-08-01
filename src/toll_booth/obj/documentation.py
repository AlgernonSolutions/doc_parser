class DocumentationEntry:
    def __init__(self, entry_handle, entry_value):
        self._entry_handle = entry_handle
        self._entry_value = entry_value

    def __str__(self):
        return f'{self._entry_handle} {self._entry_value}'

    @property
    def entry_handle(self):
        return self._entry_handle

    @property
    def entry_value(self):
        return self._entry_value
