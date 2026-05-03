import sys, os, time, logging, requests, pandas as pd
from pathlib import Path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")))
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s")
log = logging.getLogger("phase2")
COORDS = {"KJFK":(40.6413,-73.7781),"KORD":(41.9742,-87.9073),"KMIA":(25.7959,-80.287),"KDFW":(32.8998,-97.0403),"KLAX":(33.9425,-118.4081),"KATL":(33.6407,-84.4277),"KDEN":(39.8561,-104.6737),"KHOU":(29.6454,-95.2789),"KAUS":(30.1975,-97.6664),"KPHL":(39.8729,-75.2437),"KBOS":(42.3643,-71.0052),"KDCA":(38.8521,-77.0377),"KLAS":(36.084,-115.1537),"KMSP":(44.8848,-93.2223),"KMSY":(29.9934,-90.258),"KOKC":(35.3931,-97.6007),"KPHX":(33.4373,-112.0078),"KSAT":(29.5337,-98.4698),"KSEA":(47.4502,-122.3088),"KSFO":(37.6213,-122.379)}
def fetch(station, dates):
    lat,lon=COORDS[station]; start=str(min(dates)); end=str(max(dates)); ds=set(str(d) for d in dates); rows=[]
    for url,label,extra in [("https://archive-api.open-meteo.com/v1/archive","OBS",{}),("https://historical-forecast-api.open-meteo.com/v1/forecast","GFS",{"models":"gfs_seamless"}),("https://historical-forecast-api.open-meteo.com/v1/forecast","ECMWF",{"models":"ecmwf_ifs025"}),("https://historical-forecast-api.open-meteo.com/v1/forecast","NBM",{"models":"ncep_nbm_conus"})]:
        try:
            time.sleep(0.25); p={"latitude":lat,"longitude":lon,"daily":"temperature_2m_max","temperature_unit":"fahrenheit","start_date":start,"end_date":end}; p.update(extra)
            r=requests.get(url,params=p,timeout=20); r.raise_for_status(); data=r.json()
            [rows.append({"station":station,"date":t,"value":v,"model":label}) for t,v in zip(data["daily"]["time"],data["daily"]["temperature_2m_max"]) if t in ds and v is not None]
        except Exception as e: log.warning("%s %s: %s",station,label,e)
    return rows
class Phase2Agent:
    def __init__(self,markets_file="data/markets.csv",output_dir="data"): self.mf=Path(markets_file); self.out=Path(output_dir)
    def run(self):
        log.info("="*60); log.info("Phase 2 - Weather Model Comparison"); log.info("="*60)
        m=pd.read_csv(self.mf); m["settlement_date"]=pd.to_datetime(m["settlement_date"]).dt.date
        rows=[]
        for s in sorted(m["station"].unique()):
            dates=sorted(m[m["station"]==s]["settlement_date"].unique())
            log.info("[%s] %d dates",s,len(dates))
            r=fetch(s,dates); rows.extend(r); log.info("  [%s] %d rows",s,len(r))
        df=pd.DataFrame(rows); df.to_csv(self.out/"forecasts_raw.csv",index=False)
        pv=df.pivot_table(index=["station","date"],columns="model",values="value",aggfunc="first").reset_index()
        pv.columns.name=None; pv=pv.rename(columns={"OBS":"observed_high_f","GFS":"GFS_high_f","ECMWF":"ECMWF_high_f","NBM":"NBM_high_f"})
        pv["date"]=pd.to_datetime(pv["date"]).dt.date
        aligned=m.merge(pv,left_on=["station","settlement_date"],right_on=["station","date"],how="left")
        aligned.to_csv(self.out/"aligned.csv",index=False)
        log.info("Saved aligned.csv (%d rows, %d cols)",len(aligned),len(aligned.columns))
        log.info("Phase 2 complete")