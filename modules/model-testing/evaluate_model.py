from scipy import stats
import numpy as np
from numpy.typing import ArrayLike

class ModelTest:
  
  def __init__(self, predict: ArrayLike, actual: ArrayLike) -> None:
    self.predict = np.array(predict)
    self.actual = np.array(actual)

  def get_error_confidence_interval(self, confidence: float):
    errors_squared = (self.predict - self.actual) ** 2
    return np.sqrt(stats.t.interval(confidence, len(errors_squared) -1, loc=errors_squared.mean(), scale=stats.sem(errors_squared)))