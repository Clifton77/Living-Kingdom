"""
Cross-validation helpers for observation data.
"""
import logging
import pandas as pd

logger = logging.getLogger(__name__)


def cross_validate_obs(
    iem_df: pd.DataFrame,
    cdo_df: pd.DataFrame,
    station: str,
    tolerance_f: float = 3.0,
) -> pd.DataFrame:
    """
    Join IEM and CDO daily max temp observations and flag discrepancies.

    Parameters
    ----------
    iem_df : DataFrame with columns [station, date, tmax_f]
    cdo_df : DataFrame with columns [station, date, tmax_f]
    station : ICAO station code (for logging)
    tolerance_f : flag rows where |IEM - CDO| > tolerance_f degrees F

    Returns
    -------
    DataFrame with columns [station, date, tmax_observed_f, source_flag]
    source_flag: "IEM" | "CDO" | "BOTH" | "CONFLICT" | "MISSING"
    """
    merged = pd.merge(
        iem_df[["date", "tmax_f"]].rename(columns={"tmax_f": "iem_tmax"}),
        cdo_df[["date", "tmax_f"]].rename(columns={"tmax_f": "cdo_tmax"}),
        on="date",
        how="outer",
    )

    results = []
    conflicts = 0

    for _, row in merged.iterrows():
        iem_val = row["iem_tmax"]
        cdo_val = row["cdo_tmax"]
        iem_nan = pd.isna(iem_val)
        cdo_nan = pd.isna(cdo_val)

        if iem_nan and cdo_nan:
            flag = "MISSING"
            value = float("nan")
        elif iem_nan:
            flag = "CDO"
            value = cdo_val
        elif cdo_nan:
            flag = "IEM"
            value = iem_val
        elif abs(iem_val - cdo_val) > tolerance_f:
            flag = "CONFLICT"
            value = iem_val  # prefer IEM
            conflicts += 1
            logger.warning(
                "%s %s: IEM=%.1f CDO=%.1f differ by %.1fF — using IEM",
                station, row["date"], iem_val, cdo_val, abs(iem_val - cdo_val)
            )
        else:
            flag = "BOTH"
            value = iem_val

        results.append({
            "station": station,
            "date": row["date"],
            "tmax_observed_f": value,
            "source_flag": flag,
        })

    if conflicts > 0:
        logger.info("%s: %d conflict(s) found, IEM preferred", station, conflicts)

    df = pd.DataFrame(results)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    return df
