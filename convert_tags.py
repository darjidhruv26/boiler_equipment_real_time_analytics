input_file = "industrial_tags.txt"
output_file = "insert.sql"

rows = []

with open(input_file, "r", encoding="utf-8") as f:
    for line in f:
        line = line.strip()

        if not line.startswith("|"):
            continue

        # remove first and last |
        parts = [p.strip() for p in line.strip("|").split("|")]

        if len(parts) != 5:
            continue

        pi_point_id = parts[0]
        equipment = parts[1]
        tag_name = parts[2]
        unit = parts[3]
        description = parts[4]

        row = f"({pi_point_id},'{equipment}','{tag_name}','{unit}','{description}')"
        rows.append(row)

with open(output_file, "w", encoding="utf-8") as f:
    f.write(
        "INSERT INTO industrial_analytics.sensor_metadata\n"
        "(pi_point_id, equipment, tag_name, unit, description)\n"
        "VALUES\n"
    )

    for i, r in enumerate(rows):
        if i < len(rows) - 1:
            f.write(r + ",\n")
        else:
            f.write(r + ";\n")

print("Done. Output -> insert.sql")
