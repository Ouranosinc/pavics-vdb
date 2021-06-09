import traceback

# TODO : hackish import from parent folder
import sys
sys.path.append('../..')
import spec2

def exp():
    try:
        path = "../../CMIP6_CVs"
        cvs = spec2.load_cvs(path)
        model = spec2.model_from_cvs("CMIP6", cvs)
        model(experiment_id="1pctCO2")
    except Exception:
        print(traceback.format_exc())
        # or
        print(sys.exc_info()[2])

if __name__ == "__main__":
    exp()
