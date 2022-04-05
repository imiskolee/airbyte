#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#


import tracemalloc

import requests_mock
from conftest import generate_stream
from source_salesforce.streams import BulkIncrementalSalesforceStream


def test_memory_download_data(stream_config, stream_api):
    job_full_url: str = "https://fase-account.salesforce.com/services/data/v52.0/jobs/query/7504W00000bkgnpQAA"
    stream: BulkIncrementalSalesforceStream = generate_stream("Account", stream_config, stream_api)

    content = b'"Id","IsDeleted"'
    for _ in range(100000):
        content += b'"0014W000027f6UwQAI","false"\n'

    with requests_mock.Mocker() as m:
        m.register_uri("GET", f"{job_full_url}/results", content=content)

        tracemalloc.start()

        for x in stream.download_data(url=job_full_url):
            pass

        first_size, first_peak = tracemalloc.get_traced_memory()
        first_size_in_megabytes = first_size / 1024**2
        first_peak_in_megabytes = first_peak / 1024**2
        print(f"first_size = {first_size_in_megabytes} Mb, first_peak = {first_peak_in_megabytes} Mb")
