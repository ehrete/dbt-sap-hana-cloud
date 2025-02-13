from dbt.adapters.saphanacloud.connections import SapHanaCloudConnectionManager # noqa
from dbt.adapters.saphanacloud.connections import SapHanaCloudCredentials
from dbt.adapters.saphanacloud.impl import SapHanaCloudAdapter

from dbt.adapters.base import AdapterPlugin
from dbt.include import saphanacloud


Plugin = AdapterPlugin(
    adapter=SapHanaCloudAdapter,
    credentials=SapHanaCloudCredentials,
    include_path=saphanacloud.PACKAGE_PATH
    )
