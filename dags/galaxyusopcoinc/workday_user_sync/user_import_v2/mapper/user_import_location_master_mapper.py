from galaxyusopcoinc.workday_user_sync.user_import_v2.mapper.user_import_mapper_per_country import india
from galaxyusopcoinc.workday_user_sync.user_import_v2.mapper.user_import_mapper_per_country import canada
from galaxyusopcoinc.workday_user_sync.user_import_v2.mapper.user_import_mapper_per_country import united_states_of_america
from galaxyusopcoinc.workday_user_sync.user_import_v2.mapper.user_import_mapper_per_country import thailand
from galaxyusopcoinc.workday_user_sync.user_import_v2.mapper.user_import_mapper_per_country import united_kingdom
from galaxyusopcoinc.workday_user_sync.user_import_v2.mapper.user_import_mapper_per_country import japan
from galaxyusopcoinc.workday_user_sync.user_import_v2.mapper.user_import_mapper_per_country import puerto_rico
from galaxyusopcoinc.workday_user_sync.user_import_v2.mapper.user_import_mapper_per_country import hong_kong
from galaxyusopcoinc.workday_user_sync.user_import_v2.mapper.user_import_mapper_per_country import netherlands
from galaxyusopcoinc.workday_user_sync.user_import_v2.mapper.user_import_mapper_per_country import australia
from galaxyusopcoinc.workday_user_sync.user_import_v2.mapper.user_import_mapper_per_country import poland
from galaxyusopcoinc.workday_user_sync.user_import_v2.mapper.user_import_mapper_per_country import austria
from galaxyusopcoinc.workday_user_sync.user_import_v2.mapper.user_import_mapper_per_country import france
from galaxyusopcoinc.workday_user_sync.user_import_v2.mapper.user_import_mapper_per_country import switzerland
from galaxyusopcoinc.workday_user_sync.user_import_v2.mapper.user_import_mapper_per_country import finland
from galaxyusopcoinc.workday_user_sync.user_import_v2.mapper.user_import_mapper_per_country import ireland
from galaxyusopcoinc.workday_user_sync.user_import_v2.mapper.user_import_mapper_per_country import germany
from galaxyusopcoinc.workday_user_sync.user_import_v2.mapper.user_import_mapper_per_country import norway
from galaxyusopcoinc.workday_user_sync.user_import_v2.mapper.user_import_mapper_per_country import mexico
from galaxyusopcoinc.workday_user_sync.user_import_v2.mapper.user_import_mapper_per_country import italy
from galaxyusopcoinc.workday_user_sync.user_import_v2.mapper.user_import_mapper_per_country import czechia
from galaxyusopcoinc.workday_user_sync.user_import_v2.mapper.user_import_mapper_per_country import philippines
from galaxyusopcoinc.workday_user_sync.user_import_v2.mapper.user_import_mapper_per_country import qatar
from galaxyusopcoinc.workday_user_sync.user_import_v2.mapper.user_import_mapper_per_country import turkey
from galaxyusopcoinc.workday_user_sync.user_import_v2.mapper.user_import_mapper_per_country import denmark
from galaxyusopcoinc.workday_user_sync.user_import_v2.mapper.user_import_mapper_per_country import luxembourg
from galaxyusopcoinc.workday_user_sync.user_import_v2.mapper.user_import_mapper_per_country import portugal
from galaxyusopcoinc.workday_user_sync.user_import_v2.mapper.user_import_mapper_per_country import kazakhstan
from galaxyusopcoinc.workday_user_sync.user_import_v2.mapper.user_import_mapper_per_country import morocco
from galaxyusopcoinc.workday_user_sync.user_import_v2.mapper.user_import_mapper_per_country import vietnam
from galaxyusopcoinc.workday_user_sync.user_import_v2.mapper.user_import_mapper_per_country import united_arab_emirates
from galaxyusopcoinc.workday_user_sync.user_import_v2.mapper.user_import_mapper_per_country import saudi_arabia
from galaxyusopcoinc.workday_user_sync.user_import_v2.mapper.user_import_mapper_per_country import hungary
from galaxyusopcoinc.workday_user_sync.user_import_v2.mapper.user_import_mapper_per_country import romania
from galaxyusopcoinc.workday_user_sync.user_import_v2.mapper.user_import_mapper_per_country import argentina
from galaxyusopcoinc.workday_user_sync.user_import_v2.mapper.user_import_mapper_per_country import belgium
from galaxyusopcoinc.workday_user_sync.user_import_v2.mapper.user_import_mapper_per_country import brazil
from galaxyusopcoinc.workday_user_sync.user_import_v2.mapper.user_import_mapper_per_country import china
from galaxyusopcoinc.workday_user_sync.user_import_v2.mapper.user_import_mapper_per_country import sweden
from galaxyusopcoinc.workday_user_sync.user_import_v2.mapper.user_import_mapper_per_country import singapore
from galaxyusopcoinc.workday_user_sync.user_import_v2.mapper.user_import_mapper_per_country import spain
from galaxyusopcoinc.workday_user_sync.user_import_v2.mapper.user_import_mapper_per_country import south_africa
from galaxyusopcoinc.workday_user_sync.user_import_v2.mapper.user_import_mapper_per_country import malaysia

# Decrepitated in the CR #02076573
MAPPER_TO_USE_FOR_COUNTRY_TRIAL = {
    "india": india.INDIA_USER_MAPPER,
    "canada": canada.CANADA_USER_MAPPER,
    "united_states_of_america": united_states_of_america.UNITED_STATES_OF_AMERICA_USER_MAPPER,
    "thailand": thailand.THAILAND_USER_MAPPER,
    "united_kingdom": united_kingdom.UNITED_KINGDOM_USER_MAPPER,
    "japan": japan.JAPAN_USER_MAPPER,
    "puerto_rico": puerto_rico.PUERTO_RICO_USER_MAPPER,
    "hong_kong": hong_kong.HONG_KONG_USER_MAPPER,
    "netherlands": netherlands.NETHERLANDS_USER_MAPPER,
    "australia": australia.AUSTRALIA_USER_MAPPER,
    "poland": poland.POLAND_USER_MAPPER,
    "austria": austria.AUSTRIA_USER_MAPPER,
    "france": france.FRANCE_USER_MAPPER,
    "switzerland": switzerland.SWITZERLAND_USER_MAPPER,
    "finland": finland.FINLAND_USER_MAPPER,
    "ireland": ireland.IRELAND_USER_MAPPER,
    "germany": germany.GERMANY_USER_MAPPER,
    "norway": norway.NORWAY_USER_MAPPER,
    "mexico": mexico.MEXICO_USER_MAPPER,
    "italy": italy.ITALY_USER_MAPPER,
    "czechia": czechia.CZECHIA_USER_MAPPER,
    "philippines": philippines.PHILIPPINES_USER_MAPPER,
    "qatar": qatar.QATAR_USER_MAPPER,
    "türkiye": turkey.TURKEY_USER_MAPPER,
    "denmark": denmark.DENMARK_USER_MAPPER,
    "luxembourg": luxembourg.LUXEMBOURG_USER_MAPPER,
    "portugal": portugal.PORTUGAL_USER_MAPPER,
    "kazakhstan": kazakhstan.KAZAKHSTAN_USER_MAPPER,
    "morocco": morocco.MOROCCO_USER_MAPPER,
    "vietnam": vietnam.VIETNAM_USER_MAPPER,
    "united_arab_emirates": united_arab_emirates.UNITED_ARAB_EMIRATES_USER_MAPPER,
    "saudi_arabia": saudi_arabia.SAUDI_ARABIA_USER_MAPPER,
    "hungary": hungary.HUNGARY_USER_MAPPER,
    "romania": romania.ROMANIA_USER_MAPPER,
    "argentina": argentina.ARGENTINA_USER_MAPPER,
    "belgium": belgium.BELGIUM_USER_MAPPER,
    "brazil": brazil.BRAZIL_USER_MAPPER,
    "china": china.CHINA_USER_MAPPER,
    "sweden": sweden.SWEDEN_USER_MAPPER,
    "singapore": singapore.SINGAPORE_USER_MAPPER,
    "spain": spain.SPAIN_USER_MAPPER,
    "south_africa": south_africa.SOUTH_AFRICA_MAPPER,
    "malaysia": malaysia.MALAYSIA_MAPPER
}

MAPPER_TO_USE_FOR_COUNTRY_PROD = {
    "india": india.INDIA_USER_MAPPER,
    "canada": canada.CANADA_USER_MAPPER,
    "united_states_of_america": united_states_of_america.UNITED_STATES_OF_AMERICA_USER_MAPPER,
    "thailand": thailand.THAILAND_USER_MAPPER,
    "united_kingdom": united_kingdom.UNITED_KINGDOM_USER_MAPPER,
    "japan": japan.JAPAN_USER_MAPPER,
    "puerto_rico": puerto_rico.PUERTO_RICO_USER_MAPPER,
    "hong_kong": hong_kong.HONG_KONG_USER_MAPPER,
    "netherlands": netherlands.NETHERLANDS_USER_MAPPER,
    "australia": australia.AUSTRALIA_USER_MAPPER,
    "poland": poland.POLAND_USER_MAPPER,
    "austria": austria.AUSTRIA_USER_MAPPER,
    "france": france.FRANCE_USER_MAPPER,
    "switzerland": switzerland.SWITZERLAND_USER_MAPPER,
    "finland": finland.FINLAND_USER_MAPPER,
    "ireland": ireland.IRELAND_USER_MAPPER,
    "germany": germany.GERMANY_USER_MAPPER,
    "norway": norway.NORWAY_USER_MAPPER,
    "mexico": mexico.MEXICO_USER_MAPPER,
    "italy": italy.ITALY_USER_MAPPER,
    "czechia": czechia.CZECHIA_USER_MAPPER,
    "philippines": philippines.PHILIPPINES_USER_MAPPER,
    "qatar": qatar.QATAR_USER_MAPPER,
    "türkiye": turkey.TURKEY_USER_MAPPER,
    "denmark": denmark.DENMARK_USER_MAPPER,
    "luxembourg": luxembourg.LUXEMBOURG_USER_MAPPER,
    "portugal": portugal.PORTUGAL_USER_MAPPER,
    "kazakhstan": kazakhstan.KAZAKHSTAN_USER_MAPPER,
    "morocco": morocco.MOROCCO_USER_MAPPER,
    "vietnam": vietnam.VIETNAM_USER_MAPPER,
    "united_arab_emirates": united_arab_emirates.UNITED_ARAB_EMIRATES_USER_MAPPER,
    "saudi_arabia": saudi_arabia.SAUDI_ARABIA_USER_MAPPER,
    "hungary": hungary.HUNGARY_USER_MAPPER,
    "romania": romania.ROMANIA_USER_MAPPER,
    "argentina": argentina.ARGENTINA_USER_MAPPER,
    "belgium": belgium.BELGIUM_USER_MAPPER,
    "brazil": brazil.BRAZIL_USER_MAPPER,
    "china": china.CHINA_USER_MAPPER,
    "sweden": sweden.SWEDEN_USER_MAPPER,
    "singapore": singapore.SINGAPORE_USER_MAPPER,
    "spain": spain.SPAIN_USER_MAPPER,
    "south_africa": south_africa.SOUTH_AFRICA_MAPPER,
    "malaysia": malaysia.MALAYSIA_MAPPER
}
