import rail
from datetime import datetime


def get_latest(record1, record2):
    time1 = datetime.strptime(record1["updated_at"], "%Y-%m-%dT%H:%M:%SZ")
    time2 = datetime.strptime(record2["updated_at"], "%Y-%m-%dT%H:%M:%SZ")
    return record2 if time2 > time1 else record1


def get_delta_records(created_records, updated_records):
    combined_records = []
    for item in created_records:
        if item in updated_records:
            combined_records.append(
                get_latest(
                    item,
                    rail.find_first_by_attr_and_get_attr(
                        updated_records, "id", item["id"]
                    ),
                )
            )
        else:
            combined_records.append(item)
    combined_record_ids = [record["id"] for record in combined_records]
    for item in updated_records:
        if item["id"] not in combined_record_ids:
            combined_records.append(item)
    return combined_records


def get_dag_conf(dag_run):
    return {
        k: v
        for k, v in dag_run.conf.items()
        if k not in ("_ancestry", "_ecid", "_replication_position")
    }
