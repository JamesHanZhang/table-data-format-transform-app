

from analysis_modules.params_monitor import OutputParams
from analysis_modules.drawpics.image_creation import ImageCreation
from analysis_modules import default_properties as prop, drawpics
from analysis_modules.df_processing import BasicProcessing
from analysis_modules.params_monitor import SysLog


class LineChartCreation(ImageCreation):
    def __init__(self, output_params: OutputParams):
        super().__init__(output_params)