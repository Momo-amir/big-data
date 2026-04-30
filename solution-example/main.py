import sys
from modules.extract import download_csv_requests, download_csv_wget, download_csv_curl
from modules.transform import filter_by_value
from modules.load import save_csv, load_dataframe_to_mysql
from modules.visualization import fetch_iris_data, scatter_plot, histogram, boxplot
from modules.security import encrypt_cbc, decrypt_cbc, encrypt_fernet, decrypt_fernet, encrypt_gcm, decrypt_gcm

CSV_URL = 'https://raw.githubusercontent.com/jbrownlee/Datasets/master/iris.csv'
CSV_FILE = 'iris.csv'
INPUT_PATH = 'src/_input/'

OUTPUT_PATH = 'src/_output/' + 'transformed_' + CSV_FILE

method = sys.argv[1] if len(sys.argv) > 1 else 'requests'

if method == 'wget':
    download_csv_wget(CSV_URL, INPUT_PATH + CSV_FILE)
    enc, dec, mode_name = encrypt_cbc, decrypt_cbc, "AES-CBC"
elif method == 'curl':
    download_csv_curl(CSV_URL, INPUT_PATH + CSV_FILE)
    enc, dec, mode_name = encrypt_fernet, decrypt_fernet, "Fernet"
elif method == 'none':
    download_csv_requests(CSV_URL, INPUT_PATH + CSV_FILE)
    enc, dec, mode_name = None, None, "none"
else:
    download_csv_requests(CSV_URL, INPUT_PATH + CSV_FILE)
    enc, dec, mode_name = encrypt_gcm, decrypt_gcm, "AES-GCM"

sample = "5.1"
if enc:
    token = enc(sample)
    recovered = dec(token)
    print(f"Security mode ({mode_name}): {token[:40]}...  →  {recovered}")
else:
    print(f"Security mode ({mode_name}): no encryption")

print(f"CSV downloaded via {method} and saved to {INPUT_PATH}")

filtered = filter_by_value(INPUT_PATH + CSV_FILE, column='species', value='Iris-setosa')
save_csv(filtered, OUTPUT_PATH, enc)
print(f"Filtered CSV saved to {OUTPUT_PATH}")

load_dataframe_to_mysql(filtered)
print("Data loaded to MySQL")

data = fetch_iris_data()
scatter_plot(
    data["sepal_length"], data["sepal_width"],
    x_label="Sepal Length", y_label="Sepal Width"
)
  
histogram(
    data["petal_width"],
    bins=10,
    x_label="Petal Width",
    title="Distribution of Petal Width"
)

boxplot(
    data["sepal_length"], data["sepal_width"],
    data["petal_length"], data["petal_width"]
)