# Velib Rebalancing Radar

[English](#english) / [Français](#français)

<a id="english"></a>
## English

Data pipeline that identifies which Velib stations need rebalancing first: the ones
that are too often empty, leaving no bike to take, or too often full, leaving no dock
to return one.

Rebalancing is expensive for an operator. Rather than treating every station the same
way, this project ranks the structurally problematic ones from regular readings, so a
field team can focus where the service is unavailable most often.

## The pipeline

Two Python scripts query the Velib GBFS API and write the result as Parquet on S3.
Snowflake imports those files into a raw schema, dbt turns them into two successive
layers, and a Streamlit app displays the final ranking. Airflow drives the whole
thing: capture every 5 minutes, load and transform every 30 minutes.

## Stack

| Layer | Tool |
|---|---|
| Extraction | Python, requests, pandas, pyarrow |
| Data lake | Amazon S3 (Parquet) |
| Warehouse | Snowflake |
| Transformation | dbt (dbt-snowflake) |
| Orchestration | Apache Airflow (Docker Compose) |
| Presentation | Streamlit |
| Code quality | Ruff |
| CI | GitHub Actions |

## How it works

The first script captures station status, which changes constantly. The second one
captures the reference data (name, position, capacity), which is nearly static, so
once a day is enough.

Each reading is converted to Parquet and dropped on S3 in a tree partitioned by date
and hour. Snowflake then imports those files with `COPY INTO`, with no hardcoded key:
a storage integration relies on a dedicated IAM role, which avoids storing an AWS
secret inside the warehouse.

dbt builds two layers. Staging renames, types and deduplicates, with 16 duplicates
removed on the first load through `ROW_NUMBER`. The marts produce an incremental fact
table, a station dimension, and the final priority table.

## Criticality score

For every station with at least ten readings:

```
score = 0.7 x percent_time_empty + 0.3 x percent_time_full
```

The empty state carries more weight because a station with no bike blocks any
departure, which hurts the user more than a full station, where a free dock is often a
few streets away. The score stays deliberately simple so a non technical operator can
read it.

## Data quality

dbt tests run on every transformation. Generic ones: uniqueness and non nullity of the
station id, allowed values (0 or 1) on the renting and returning flags, referential
integrity between the fact table and the dimension.

Two custom tests on top. The first checks there is no duplicate on the station id and
timestamp pair. The second checks consistency between declared capacity and the sum of
available bikes and docks, but as a warning rather than an error: the source contains
occasional inconsistencies that are not worth treating as a pipeline failure.

## Orchestration

Two DAGs run in a local Docker Compose stack. `velib_capture` triggers the capture and
the upload to S3 every 5 minutes. `velib_load_dbt` loads S3 into Snowflake then
rebuilds both dbt layers every 30 minutes.

The Airflow image is extended by a Dockerfile that installs dbt, and the repo code is
mounted through volumes, which avoids copying anything by hand on each change.

## Continuous integration

On every push, GitHub Actions checks Python style with Ruff and validates the dbt
project structure with `dbt parse`. The validation uses a fake profile: no real
Snowflake connection, so no secret is exposed in the pipeline.

## Code layout

The extraction scripts and the S3 upload helper live in `extraction`. The Snowflake
setup and raw loading script is in `snowflake`. The dbt project occupies `dbt_velib`,
with sources, staging, marts, tests and macros. The Airflow stack (Docker Compose,
Dockerfile, DAGs) is in `airflow`, the Streamlit app in `dashboard`, and the CI
workflow in `.github`.

## Running it

Requirements: Python 3.12, Docker with at least 4 GB of RAM, an AWS account, a
Snowflake account.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Credentials

Each tool reads its own configuration file. The repo ships a template for each one:
copy it, then fill in your values. The copies stay out of git, only the templates are
versioned.

| Template | Copy to | Used by |
|---|---|---|
| `.env.example` | `.env` | extraction scripts |
| `airflow/.env.example` | `airflow/.env` | Airflow containers |
| `dbt_velib/profiles.yml.example` | `~/.dbt/profiles.yml` | dbt (the Airflow containers mount this same file) |
| `.streamlit/secrets.toml.example` | `.streamlit/secrets.toml` | Streamlit app |

The `FERNET_KEY` value in `airflow/.env` can be generated with:

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

### AWS and Snowflake

Create an S3 bucket, then an IAM role that Snowflake will assume to read it. Open
`snowflake/setup.sql`, replace the bucket name and the role ARN with yours, and run
the script in a Snowflake worksheet: it creates the warehouse, the database, the
three schemas, the raw tables, the storage integration and the stage.

The integration then has to be authorized on the AWS side. `DESC STORAGE INTEGRATION
velib_s3_stage_int` returns a `STORAGE_AWS_IAM_USER_ARN` and a
`STORAGE_AWS_EXTERNAL_ID`, both of which go into the trust policy of the IAM role.
The Snowflake documentation describes this exchange step by step.

### Launch

From the repo root:

```bash
# manual extraction
python extraction/extract_station_status.py
python extraction/extract_station_information.py

# transformations
cd dbt_velib
dbt run
dbt test
cd ..

# orchestration
cd airflow
docker compose up -d
cd ..

# dashboard
streamlit run dashboard/app.py
```

The Airflow UI runs on http://localhost:8080 (default login and password: airflow).
DAGs are paused when first created: switch on `velib_capture` and `velib_load_dbt`
from the UI.

## Technical choices

No Kafka and no streaming: the source is an API polled at moderate volume. Micro batch
answers the need without pointless complexity.

S3 with partitioned Parquet rather than a regular database for the raw layer: low
cost, efficient columnar format, simple filtering by date and hour.

A Snowflake storage integration rather than hardcoded keys: the warehouse reaches S3
through an IAM role, so no AWS secret is stored inside it.

## Limits

The score does not account for hourly variation or seasonality yet. An empty station
at 8am does not mean the same thing as an empty station at 3am.

A rolling window over 7 or 30 days would give a more representative ranking than a
cumulative count since the first reading.

<a id="français"></a>
## Français

Pipeline de données qui identifie les stations Vélib à rééquilibrer en priorité : celles qui sont trop souvent vides, donc sans vélo à emprunter, ou trop souvent pleines, donc sans place pour en restituer un.

Rééquilibrer coûte cher à un opérateur. Plutôt que de traiter toutes les stations de la même façon, le projet produit un classement des stations structurellement problématiques à partir de relevés réguliers, pour qu'une équipe terrain concentre ses efforts là où le service est le plus souvent indisponible.

## Le pipeline

Deux scripts Python interrogent l'API GBFS de Vélib et écrivent le résultat en Parquet sur S3. Snowflake importe ces fichiers dans un schéma brut, dbt les transforme en deux couches successives, et une application Streamlit affiche le classement final. Airflow orchestre l'ensemble : capture toutes les 5 minutes, chargement et transformation toutes les 30 minutes.

## Stack technique

| Couche | Outil |
|---|---|
| Extraction | Python, requests, pandas, pyarrow |
| Data lake | Amazon S3 (Parquet) |
| Entrepôt | Snowflake |
| Transformation | dbt (dbt-snowflake) |
| Orchestration | Apache Airflow (Docker Compose) |
| Restitution | Streamlit |
| Qualité de code | Ruff |
| Intégration continue | GitHub Actions |

## Fonctionnement

Le premier script capture l'état des stations, une donnée qui change en permanence. Le second capture le référentiel (nom, position, capacité), quasi statique, une fois par jour suffit.

Chaque relevé est converti en Parquet et déposé sur S3 dans une arborescence partitionnée par date et par heure. Snowflake importe ensuite ces fichiers avec des commandes `COPY INTO`, sans aucune clé en dur : une storage integration s'appuie sur un rôle IAM dédié, ce qui évite de stocker un secret AWS dans l'entrepôt.

dbt construit deux couches. Le staging renomme, type et déduplique, avec 16 doublons éliminés sur le premier chargement via `ROW_NUMBER`. Les marts produisent une table de faits incrémentale, une dimension station, et la table finale de priorité.

## Score de criticité

Pour chaque station ayant au moins dix relevés :

```
score = 0,7 x pourcentage_temps_vide + 0,3 x pourcentage_temps_pleine
```

Le poids sur l'état vide est plus élevé parce qu'une station sans vélo empêche tout départ, ce qui pénalise plus l'usager qu'une station pleine, où une place libre se trouve souvent à quelques rues. Le score reste volontairement simple pour qu'un opérateur non technique puisse l'interpréter.

## Qualité des données

Les tests dbt tournent à chaque transformation. Côté générique : unicité et non-nullité de l'identifiant de station, valeurs autorisées (0 ou 1) sur les indicateurs de location et de restitution, intégrité référentielle entre la table de faits et la dimension.

Deux tests personnalisés en plus. Le premier vérifie l'absence de doublon sur le couple identifiant et horodatage. Le second contrôle la cohérence entre la capacité déclarée et la somme des vélos et places disponibles, mais en avertissement plutôt qu'en erreur : la source contient parfois des incohérences ponctuelles qu'il ne sert à rien de traiter comme un échec de pipeline.

## Orchestration

Deux DAG tournent dans une stack Docker Compose locale. `velib_capture` déclenche la capture et l'envoi vers S3 toutes les 5 minutes. `velib_load_dbt` charge S3 vers Snowflake puis reconstruit les deux couches dbt toutes les 30 minutes.

L'image Airflow est étendue par un Dockerfile qui installe dbt, et le code du dépôt est monté par des volumes, ce qui évite toute copie manuelle à chaque modification.

## Intégration continue

À chaque push, GitHub Actions vérifie le style du code Python avec Ruff et valide la structure du projet dbt avec `dbt parse`. La validation utilise un profil factice : pas de connexion réelle à Snowflake, donc aucun secret exposé dans le pipeline.

## Organisation du code

Les scripts d'extraction et la fonction d'upload S3 sont dans `extraction`. Le script de configuration Snowflake et de chargement brut est dans `snowflake`. Le projet dbt occupe `dbt_velib`, avec sources, staging, marts, tests et macros. La stack Airflow (Docker Compose, Dockerfile, DAG) est dans `airflow`, l'application Streamlit dans `dashboard`, et le workflow d'intégration continue dans `.github`.

## Lancer le projet

Prérequis : Python 3.12, Docker avec au moins 4 Go de RAM, un compte AWS, un compte Snowflake.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Identifiants

Chaque outil lit son propre fichier de configuration. Le dépôt fournit un modèle pour chacun : copiez-le puis remplissez vos valeurs. Les copies restent hors de git, seuls les modèles sont versionnés.

| Modèle | À copier vers | Utilisé par |
|---|---|---|
| `.env.example` | `.env` | scripts d'extraction |
| `airflow/.env.example` | `airflow/.env` | conteneurs Airflow |
| `dbt_velib/profiles.yml.example` | `~/.dbt/profiles.yml` | dbt (les conteneurs Airflow montent ce même fichier) |
| `.streamlit/secrets.toml.example` | `.streamlit/secrets.toml` | application Streamlit |

La valeur `FERNET_KEY` de `airflow/.env` se génère avec :

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

### AWS et Snowflake

Créez un bucket S3, puis un rôle IAM que Snowflake assumera pour le lire. Ouvrez `snowflake/setup.sql`, remplacez le nom du bucket et l'ARN du rôle par les vôtres, puis exécutez le script dans une worksheet Snowflake : il crée le warehouse, la base, les trois schémas, les tables brutes, la storage integration et le stage.

Il reste à autoriser l'intégration côté AWS. `DESC STORAGE INTEGRATION velib_s3_stage_int` renvoie un `STORAGE_AWS_IAM_USER_ARN` et un `STORAGE_AWS_EXTERNAL_ID`, à reporter tous les deux dans la trust policy du rôle IAM. La documentation Snowflake décrit cet échange pas à pas.

### Démarrage

Depuis la racine du dépôt :

```bash
# extraction manuelle
python extraction/extract_station_status.py
python extraction/extract_station_information.py

# transformations
cd dbt_velib
dbt run
dbt test
cd ..

# orchestration
cd airflow
docker compose up -d
cd ..

# tableau de bord
streamlit run dashboard/app.py
```

L'interface Airflow tourne sur http://localhost:8080 (identifiant et mot de passe par défaut : airflow). Les DAG sont en pause à leur création : activez `velib_capture` et `velib_load_dbt` depuis l'interface.

## Choix techniques

Pas de Kafka ni de streaming : la source est une API interrogée en polling, à volume modéré. Le micro batch répond au besoin sans complexité inutile.

S3 avec du Parquet partitionné plutôt qu'une base classique pour la couche brute : coût faible, format colonne efficace, filtrage simple par date et par heure.

La storage integration Snowflake plutôt que des clés en dur : l'entrepôt accède à S3 via un rôle IAM, il n'y a aucun secret AWS stocké dedans.

## Limites

Le score n'intègre pas encore la variation horaire ni la saisonnalité. Une station vide à 8h du matin n'a pas le même sens qu'une station vide à 3h.

Une fenêtre glissante sur 7 ou 30 jours donnerait un classement plus représentatif qu'un cumul depuis le début des relevés.
