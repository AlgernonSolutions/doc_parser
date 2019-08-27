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


class DocumentationTextEntry:
    def __init__(self,
                 encounter_internal_id,
                 encounter_type,
                 id_source,
                 documentation_text,
                 provider_internal_id,
                 patient_internal_id,
                 provider_id_value,
                 patient_id_value,
                 encounter_id_value):
        self._encounter_internal_id = encounter_internal_id
        self._encounter_type = encounter_type
        self._id_source = id_source
        self._documentation_text = documentation_text
        self._provider_internal_id = provider_internal_id
        self._patient_internal_id = patient_internal_id
        self._provider_id_value = provider_id_value
        self._patient_id_value = patient_id_value
        self._encounter_id_value = encounter_id_value

    @property
    def documentation_text(self):
        return self._documentation_text

    @property
    def encounter_internal_id(self):
        return self._encounter_internal_id

    @property
    def encounter_type(self):
        return self._encounter_type

    @property
    def id_source(self):
        return self._id_source

    @property
    def provider_internal_id(self):
        return self._provider_internal_id

    @property
    def patient_internal_id(self):
        return self._patient_internal_id

    @property
    def provider_id_value(self):
        return self._provider_id_value

    @property
    def patient_id_value(self):
        return self._patient_id_value

    @property
    def for_insertion(self):
        return {
            'encounter_internal_id': self._encounter_internal_id,
            'encounter_type': self._encounter_type,
            'id_source': self._id_source,
            'documentation_text': self._documentation_text,
            'provider_internal_id': self._provider_internal_id,
            'patient_internal_id': self._patient_internal_id,
            'provider_id_value': self._provider_id_value,
            'patient_id_value': self._patient_id_value,
            'encounter_id_value': self._encounter_id_value
        }

    def __getitem__(self, item):
        result = getattr(self, f'_{item}', None)
        if result is None:
            raise KeyError
        return result
