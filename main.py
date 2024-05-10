import os
import glob
import shutil
import sys

from geojson import Feature, FeatureCollection, Point, dumps

def is_not_matchless(path: str) -> bool:
    return not path.endswith("_matchless.parquet")

def run (root_matches_directory, partials_dir):
  all_pairs = glob.glob("*_pairs", root_dir=root_matches_directory)

  id = 0

  for idx, pair in enumerate(all_pairs):
      matches_directory = os.path.join(root_matches_directory, pair)

      output_directory = os.path.join(partials_dir, pair)

      os.makedirs(output_directory, exist_ok=True)

      # Copy geojson project file
      shutil.copyfile(
          os.path.join(root_matches_directory, pair.replace("_pairs", "") + ".geojson"), 
          os.path.join(partials_dir, pair.replace("_pairs", "") + ".geojson"), 
      )

      # Copy configuration file
      shutil.copyfile(
          os.path.join("./tmf-data/configurations/", pair.replace("_pairs", "") + ".json"), 
          os.path.join(partials_dir, pair.replace("_pairs", "") + ".json"), 
      )

      print(f"Procressing {idx + 1} / {len(all_pairs)} ({matches_directory})")

      matches = glob.glob("*.parquet", root_dir=matches_directory)
      matches = [x for x in matches if is_not_matchless(x)]

      for pair_idx, pairs in enumerate([matches[0]]):
          matches_df = pd.read_parquet(os.path.join(matches_directory, pairs))

          years = [col.replace("k_luc_", "") for col in matches_df.columns if "k_luc_" in col]

          points = []

          print(matches_df.columns)
          for _, row in matches_df.iterrows():

              treat_props = { 
                  "slope": row["k_slope"],
                  "elevation": row["k_elevation"],
                  "accessibility": row["k_access"],
                  "treatment": "treatment",
                  "id": id
              }

              for year in years:
                  treat_props["Dec" + year] = row["k_luc_" + year]

              treatment = Feature(
                  properties=treat_props,
                  geometry=Point((row["k_lng"], row["k_lat"]))
              )

              id = id + 1

              control_props = { 
                  "slope": row["s_slope"],
                  "elevation": row["s_elevation"],
                  "accessibility": row["s_access"],
                  "treatment": "control",
                  "treat_id": id - 1,
                  "id": id
              }

              for year in years:
                  control_props["Dec" + year] = row["s_luc_" + year]

              control = Feature(
                  properties=control_props,
                  geometry=Point((row["s_lng"], row["s_lat"]))
              )

              points.append(treatment)
              points.append(control)

          points_gc = FeatureCollection(points)
          out_path = os.path.join(
              output_directory, os.path.splitext(pairs)[0] + "-pairs.geojson"
          )

          with open(out_path, "w", encoding="utf-8") as output_file:
              output_file.write(dumps(points_gc))
              

def main():
    try:
        partials_dir = sys.argv[1]
        output_dir = sys.argv[2]
    except IndexError:
        print(f"Usage: {sys.argv[0]} PAIRS_DIRECTORY OUTPUT_DIRECTORY", file=sys.stderr)
        sys.exit(1)

    try:
        run(partials_dir, output_dir)
    except ValueError as exc:
        print(f"Invalid value: {exc.args}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()