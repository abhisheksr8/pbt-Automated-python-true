from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(self, c_string: str=None, c_int: int=None, c_boolean: bool=None, **kwargs):
        self.spark = None
        self.update(c_string, c_int, c_boolean)

    def update(
            self,
            c_string: str="string$$%^&*#@",
            c_int: int=65530,
            c_boolean: bool=True,
            **kwargs
    ):
        prophecy_spark = self.spark
        self.c_string = c_string
        self.c_int = self.get_int_value(c_int)
        self.c_boolean = self.get_bool_value(c_boolean)
        pass
