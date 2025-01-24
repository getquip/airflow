
import pandas as pd
with open("clients/stord/outb_Fulfillment_billing_detail_2731_102124_P.csv") as f:
	df = pd.read_csv(f)

from custom_packages import airbud

records = airbud.clean