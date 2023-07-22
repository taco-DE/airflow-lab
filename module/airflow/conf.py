import logging
from dataclasses import dataclass
from typing import List

from airflow.models import Variable


@dataclass
class AirflowDagConf:
    key: str
    default_val: str

    @property
    def variable(self):
        val = Variable.get(self.key, default_var=self.default_val)
        return val


DAG_CONFIGURATIONS: List[AirflowDagConf] = [
    AirflowDagConf("TEST_ENV", None),
    AirflowDagConf("TEST_ENV1", None),
    AirflowDagConf("TEST_ENV2", None),
    AirflowDagConf("TEST_ENV3", None),
    AirflowDagConf("TEST_ENV4", None),
    AirflowDagConf("TEST_ENV5", None),
    AirflowDagConf("TEST_ENV6", None),
    AirflowDagConf("TEST_ENV7", None),
    AirflowDagConf("TEST_ENV8", None),
    AirflowDagConf("TEST_ENV9", None),
    AirflowDagConf("TEST_ENV0", None),
    AirflowDagConf("TEST_ENV01", None),
    AirflowDagConf("TEST_ENV11", None),
    AirflowDagConf("TEST_ENV12", None),
    AirflowDagConf("TEST_ENV13", None),
    AirflowDagConf("TEST_ENV14", None),
    AirflowDagConf("TEST_ENV15", None),
    AirflowDagConf("TEST_ENV16", None),
    AirflowDagConf("TEST_ENV17", None),
    AirflowDagConf("TEST_ENV18", None),
    AirflowDagConf("TEST_ENV19", None),
    AirflowDagConf("TEST_ENV20", None),
    AirflowDagConf("TEST_ENV22", None),
]


class AirflowConf:
    def __init__(self):
        self.key_prefix = "TEST_"
        self.conf_dict = dict((c.key, c.variable) for c in DAG_CONFIGURATIONS)
        self.conf_dict[f"{self.key_prefix}TZ"] = "Asia/Seoul"

        self.print_all()

    def get(self, conf_key: str) -> str:
        val = self.conf_dict.get(f"{self.key_prefix}{conf_key}")
        if not val:
            raise RuntimeError(f"Can't find proprer variable for {conf_key}")

        return val

    def print_all(self):
        for key, val in self.conf_dict.items():
            logging.info(f"{key} => {val}")
