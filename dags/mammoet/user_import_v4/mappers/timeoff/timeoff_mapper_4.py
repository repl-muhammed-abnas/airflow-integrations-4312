from mammoet.user_import_v4.mappers.timeoff.country.belgium import BELGIUM_TIMEOFF_MAPPER
from mammoet.user_import_v4.mappers.timeoff.country.netherlands import NETHERLANDS_TIMEOFF_MAPPER
from mammoet.user_import_v4.mappers.timeoff.country.uk_v1 import UK_TIMEOFF_MAPPER
from mammoet.user_import_v4.mappers.timeoff.country.spain import SPAIN_TIMEOFF_MAPPER
from mammoet.user_import_v4.mappers.timeoff.country.france import FRANCE_TIMEOFF_MAPPER
from mammoet.user_import_v4.mappers.timeoff.country.italy_v1 import ITALY_TIMEOFF_MAPPER
from mammoet.user_import_v4.mappers.timeoff.country.us import US_TIMEOFF_MAPPER
from mammoet.user_import_v4.mappers.timeoff.country.mexico import MEXICO_TIMEOFF_MAPPER


TIMEOFF_MAPPER  = BELGIUM_TIMEOFF_MAPPER + NETHERLANDS_TIMEOFF_MAPPER + UK_TIMEOFF_MAPPER + SPAIN_TIMEOFF_MAPPER

TIMEOFF_MAPPER_UAT  = BELGIUM_TIMEOFF_MAPPER + NETHERLANDS_TIMEOFF_MAPPER + UK_TIMEOFF_MAPPER + SPAIN_TIMEOFF_MAPPER + FRANCE_TIMEOFF_MAPPER + ITALY_TIMEOFF_MAPPER + US_TIMEOFF_MAPPER + MEXICO_TIMEOFF_MAPPER
