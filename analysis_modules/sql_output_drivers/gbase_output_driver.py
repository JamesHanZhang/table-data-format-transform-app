# self-made modules
from analysis_modules.sql_output_drivers.mysql_output_driver import MySqlOutputDriver
from analysis_modules.params_monitor import OutputParams

class GBaseOutputDriver(MySqlOutputDriver):
    def __init__(self, output_params: OutputParams):
        super().__init__(output_params)