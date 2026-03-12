"""
Team Name Normalization Map
============================
Maps CBBD team names to KenPom team names so the two datasets join cleanly.

The diagnostic showed these teams missing KenPom ratings due to name mismatches:
Michigan State, Oregon State, UConn, NC State, Florida State, Kansas State,
Penn State, Washington State, Wichita State, Iowa State, Hawai'i, Ohio State,
Arizona State, Ole Miss, Mississippi State, Miami

Usage:
    from team_name_map import CBBD_TO_KENPOM, normalize_for_kenpom

    kenpom_name = normalize_for_kenpom("UConn")  # -> "Connecticut"
"""

# Maps CBBD name -> KenPom name
CBBD_TO_KENPOM = {
    # From diagnostic mismatches
    "UConn"                     : "Connecticut",
    "Ole Miss"                  : "Mississippi",
    "Hawai'i"                   : "Hawaii",
    "Miami"                     : "Miami FL",
    "Miami (OH)"                : "Miami OH",
    "Miami (FL)"                : "Miami FL",

    # State schools
    "Michigan St."              : "Michigan State",
    "Michigan St"               : "Michigan State",
    "Oregon St."                : "Oregon State",
    "Oregon St"                 : "Oregon State",
    "Florida St."               : "Florida State",
    "Florida St"                : "Florida State",
    "Kansas St."                : "Kansas State",
    "Kansas St"                 : "Kansas State",
    "Penn St."                  : "Penn State",
    "Penn St"                   : "Penn State",
    "Washington St."            : "Washington State",
    "Washington St"             : "Washington State",
    "Wichita St."               : "Wichita State",
    "Wichita St"                : "Wichita State",
    "Iowa St."                  : "Iowa State",
    "Iowa St"                   : "Iowa State",
    "Ohio St."                  : "Ohio State",
    "Ohio St"                   : "Ohio State",
    "Arizona St."               : "Arizona State",
    "Arizona St"                : "Arizona State",
    "Mississippi St."           : "Mississippi State",
    "Mississippi St"            : "Mississippi State",
    "Appalachian St."           : "Appalachian State",
    "Appalachian St"            : "Appalachian State",
    "Boise St."                 : "Boise State",
    "Boise St"                  : "Boise State",
    "Colorado St."              : "Colorado State",
    "Colorado St"               : "Colorado State",
    "Fresno St."                : "Fresno State",
    "Fresno St"                 : "Fresno State",
    "Idaho St."                 : "Idaho State",
    "Idaho St"                  : "Idaho State",
    "Kent St."                  : "Kent State",
    "Kent St"                   : "Kent State",
    "McNeese St."               : "McNeese State",
    "McNeese St"                : "McNeese State",
    "Morgan St."                : "Morgan State",
    "Morgan St"                 : "Morgan State",
    "Nicholls St."              : "Nicholls State",
    "Nicholls St"               : "Nicholls State",
    "Norfolk St."               : "Norfolk State",
    "Norfolk St"                : "Norfolk State",
    "Portland St."              : "Portland State",
    "Portland St"               : "Portland State",
    "Utah St."                  : "Utah State",
    "Utah St"                   : "Utah State",
    "Chicago St."               : "Chicago State",
    "Chicago St"                : "Chicago State",
    "Cleveland St."             : "Cleveland State",
    "Cleveland St"              : "Cleveland State",
    "Coppin St."                : "Coppin State",
    "Coppin St"                 : "Coppin State",
    "Delaware St."              : "Delaware State",
    "Delaware St"               : "Delaware State",
    "Jacksonville St."          : "Jacksonville State",
    "Jacksonville St"           : "Jacksonville State",
    "Alabama St."               : "Alabama State",
    "Alabama St"                : "Alabama State",
    "Northwestern St."          : "Northwestern State",
    "Northwestern St"           : "Northwestern State",
    "South Carolina St."        : "South Carolina State",
    "South Carolina St"         : "South Carolina State",
    "South Dakota St."          : "South Dakota State",
    "South Dakota St"           : "South Dakota State",
    "North Dakota St."          : "North Dakota State",
    "North Dakota St"           : "North Dakota State",
    "Weber St."                 : "Weber State",
    "Weber St"                  : "Weber State",
    "Kennesaw St."              : "Kennesaw State",
    "Kennesaw St"               : "Kennesaw State",
    "Kennessaw St."             : "Kennesaw State",
    "Kennessaw St"              : "Kennesaw State",
    "Sacramento St."            : "Sacramento State",
    "Sacramento St"             : "Sacramento State",
    "Youngstown St."            : "Youngstown State",
    "Youngstown St"             : "Youngstown State",
    "Murray St."                : "Murray State",
    "Murray St"                 : "Murray State",
    "Indiana St."               : "Indiana State",
    "Indiana St"                : "Indiana State",
    "Morehead St."              : "Morehead State",
    "Morehead St"               : "Morehead State",
    "Austin Peay St."           : "Austin Peay",
    "Austin Peay St"            : "Austin Peay",
    "Alcorn St."                : "Alcorn State",
    "Alcorn St"                 : "Alcorn State",
    "Grambling St."             : "Grambling State",
    "Grambling St"              : "Grambling State",
    "Jackson St."               : "Jackson State",
    "Wichita State"             : "Wichita State",

    # Abbreviations
    "TCU"                       : "TCU",
    "UAB"                       : "UAB",
    "UTEP"                      : "UTEP",
    "UTSA"                      : "UTSA",
    "UTRGV"                     : "UT Rio Grande Valley",
    "UMKC"                      : "Kansas City",
    "SIUE"                      : "SIU Edwardsville",
    "SIU Edwardsville"          : "SIU Edwardsville",
    "FAU"                       : "Florida Atlantic",
    "FIU"                       : "Florida International",
    "FGCU"                      : "Florida Gulf Coast",
    "IUPUI"                     : "IU Indianapolis",
    "LIU"                       : "Long Island",
    "LIU Brooklyn"              : "Long Island",
    "UNCG"                      : "UNC Greensboro",
    "UNCW"                      : "UNC Wilmington",
    "UIW"                       : "Incarnate Word",
    "NIU"                       : "Northern Illinois",
    "ETSU"                      : "East Tennessee State",
    "VCU"                       : "VCU",
    "UCF"                       : "UCF",
    "USC"                       : "Southern California",
    "LSU"                       : "LSU",
    "SMU"                       : "SMU",
    "BYU"                       : "BYU",
    "UIC"                       : "Illinois Chicago",
    "UMBC"                      : "UMBC",
    "URI"                       : "Rhode Island",

    # Formal name differences
    "Saint John's"              : "St. John's",
    "Saint John's (NY)"         : "St. John's",
    "St. John's (NY)"           : "St. John's",
    "St. Joseph's"              : "Saint Joseph's",
    "Saint Francis (PA)"        : "Saint Francis",
    "St. Francis (PA)"          : "Saint Francis",
    "Saint Francis U"           : "Saint Francis",
    "Loyola (IL)"               : "Loyola Chicago",
    "Loyola (MD)"               : "Loyola Maryland",
    "Sam Houston"               : "Sam Houston State",
    "Sam Houston St."           : "Sam Houston State",
    "Sam Houston St"            : "Sam Houston State",
    "Southeast Missouri"        : "Southeast Missouri State",
    "SEMO"                      : "Southeast Missouri State",
    "SE Missouri St."           : "Southeast Missouri State",
    "Southeastern Louisiana"    : "Southeastern Louisiana",
    "SE Louisiana"              : "Southeastern Louisiana",
    "Southeastern La."          : "Southeastern Louisiana",
    "Prairie View"              : "Prairie View A&M",
    "PV A&M"                    : "Prairie View A&M",
    "Southern"                  : "Southern University",
    "Southern U."               : "Southern University",
    "Southern Univ"             : "Southern University",
    "Central Connecticut"       : "Central Connecticut",
    "Cent. Conn. St."           : "Central Connecticut",
    "Queens"                    : "Queens (NC)",
    "Queens NC"                 : "Queens (NC)",
    "La. Tech"                  : "Louisiana Tech",
    "La Tech"                   : "Louisiana Tech",
    "NC Central"                : "North Carolina Central",
    "W. Carolina"               : "Western Carolina",
    "W Carolina"                : "Western Carolina",
    "W. Michigan"               : "Western Michigan",
    "W Michigan"                : "Western Michigan",
    "N. Illinois"               : "Northern Illinois",
    "E. Michigan"               : "Eastern Michigan",
    "E Michigan"                : "Eastern Michigan",
    "C. Michigan"               : "Central Michigan",
    "C Michigan"                : "Central Michigan",
    "N. Dakota St."             : "North Dakota State",
    "S. Carolina St."           : "South Carolina State",
    "S Illinois"                : "Southern Illinois",
    "Miss. Valley St."          : "Mississippi Valley State",
    "MS Valley St."             : "Mississippi Valley State",
    "MD E Shore"                : "Maryland Eastern Shore",
    "Tex. A&M-Commerce"         : "East Texas A&M",
    "Texas A&M-CC"              : "Texas A&M Corpus Christi",
    "A&M-Corpus Christi"        : "Texas A&M Corpus Christi",
    "Texas A&M-Corpus Christi"  : "Texas A&M Corpus Christi",
    "Arkansas-LR"               : "Arkansas Little Rock",
    "Ark.-Pine Bluff"           : "Arkansas Pine Bluff",
    "Arkansas-Pine Bluff"       : "Arkansas Pine Bluff",
    "Houston Chr."              : "Houston Christian",
    "Houston Chr"               : "Houston Christian",
    "Farleigh Dickinson"        : "Fairleigh Dickinson",
    "F Dickinson"               : "Fairleigh Dickinson",
    "Long Beach St."            : "Long Beach State",
    "Long Beach St"             : "Long Beach State",
    "NC State"                  : "NC State",
}

KENPOM_TO_CBBD = {v: k for k, v in CBBD_TO_KENPOM.items()}


def normalize_for_kenpom(cbbd_name: str) -> str:
    """Convert a CBBD team name to its KenPom equivalent."""
    return CBBD_TO_KENPOM.get(cbbd_name, cbbd_name)


def normalize_for_cbbd(kenpom_name: str) -> str:
    """Convert a KenPom team name to its CBBD equivalent."""
    return KENPOM_TO_CBBD.get(kenpom_name, kenpom_name)


if __name__ == "__main__":
    print(f"Total mappings: {len(CBBD_TO_KENPOM)}")
    print("\nKey diagnostic matches:")
    samples = [
        "UConn", "Ole Miss", "Hawai'i", "Michigan St.", "Miami",
        "NC State", "IUPUI", "Sam Houston", "Wichita St.", "Arizona St."
    ]
    for name in samples:
        mapped = normalize_for_kenpom(name)
        status = "MAPPED" if mapped != name else "same"
        print(f"  {name:<30} -> {mapped:<30} [{status}]")
