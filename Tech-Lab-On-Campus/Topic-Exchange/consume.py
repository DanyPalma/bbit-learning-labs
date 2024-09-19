# Copyright 2024 Bloomberg Finance L.P.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import sys

from solution.consumer_sol import mqConsumer  # pylint: disable=import-error

def main(sectors: str, tickers: str, queueName: str) -> None:
    
    # Implement Logic to Create Binding Key from the ticker and sector variable -  Step 2
    #
    #                       WRITE CODE HERE!!!
    #
    """
    Implement Logic to Create Binding Key from the variables. When binding the Queue to the exchange ensure that routing key utilizies one of the special cases for binding keys such as * (star) or # (hash). For instance. "#.Google.#" as a binding key for a Queue will ensure that all messages with a key containing the word Google such as Stock.Google.tech will have their messages routed to that Queue. Please consult with your partner and agree on what the binding and routing key should look like first!
    """
    
    bindingKeys = [f"#.{sector}.#" for sector in sectors]
    
    bindingKeys.extend([f"#.{ticker}.#" for ticker in tickers])
    
    consumer = mqConsumer(binding_keys=bindingKeys,exchange_name="Tech Lab Topic Exchange",queue_name=queueName)    
    consumer.startConsuming()
    


if __name__ == "__main__":

    # Implement Logic to read the sector and queueName string from the command line and save them - Step 1
    #
    #                       WRITE CODE HERE!!!
    #
    sectors = sys.argv[1].split(",")
    tickers = sys.argv[2].split(",")
    queue = sys.argv[3]

    sys.exit(main(sectors, tickers, queue))
