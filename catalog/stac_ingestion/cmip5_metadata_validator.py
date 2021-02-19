


class MetadataValidator(object):
    def run(self, metadata):
        self.validate(metadata)

    def validate(self, metadata):
        exceptions = ["missing optional field", "missing required field"]

        return