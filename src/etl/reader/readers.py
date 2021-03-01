import json


class Reader():
    @classmethod
    def read_file(cls, filename, *args, **kwargs):
        with open(filename, "r",  *args, **kwargs) as f:
            content = f.read()
        return content

    @classmethod
    def read_json(cls, filename, *args, **kwargs):
        with open(filename, "r", *args, **kwargs) as f:
            content = json.load(f)
        return content