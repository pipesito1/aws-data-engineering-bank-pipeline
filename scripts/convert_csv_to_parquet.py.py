import pyarrow.parquet as pq
import pyarrow as pa
import s3fs
import re

# Conexión a S3
fs = s3fs.S3FileSystem()

# Paths
input_root = "s3://felipe-data-engineering-proyecto/processed/accounts_parquet_v2/"
output_root = "s3://felipe-data-engineering-proyecto/processed/accounts_parquet_v5/"

# Listar carpetas
folders = fs.ls(input_root)

tables = []

print("Iniciando reconstrucción v5...")

for folder in folders:

    if "bank_group=" in folder:

        # Extraer valor partition
        match = re.search(r"bank_group=(\d+)", folder)
        bank_group_value = int(match.group(1))

        files = fs.ls(folder)

        for file in files:

            if file.endswith(".parquet"):

                print("Leyendo:", file)
                table = pq.read_table(
                    "s3://" + file,
                    filesystem=fs
                )

                # Quitar columna vieja si existe
                if "bank_group" in table.column_names:
                    table = table.drop(["bank_group"])

                # Crear columna nueva correcta
                bg_array = pa.array(
                    [bank_group_value] * table.num_rows,
                    type=pa.int32()
                )

                table = table.append_column("bank_group", bg_array)

                tables.append(table)


print("Total archivos leídos:", len(tables))

# Unir todo
final_table = pa.concat_tables(tables)

print("Filas totales:", final_table.num_rows)

# Guardar limpio
pq.write_to_dataset(
    final_table,
    root_path=output_root,
    partition_cols=["bank_group"],
    filesystem=fs
)

print("✅ Dataset v5 generado correctamente")