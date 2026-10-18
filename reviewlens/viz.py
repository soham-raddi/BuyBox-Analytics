"""Common matplotlib styling so every figure in the report looks consistent."""
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

BLUE, ORANGE, GREEN, RED, PURPLE, GREY = "#2E6FBA", "#E08A2B", "#3A9A5B", "#C8423B", "#7B5EA7", "#8A8F98"
RATING_COLORS = {1: "#C8423B", 2: "#E07B53", 3: "#C9B458", 4: "#7FB069", 5: "#3A9A5B"}

plt.rcParams.update({
    "figure.dpi": 110, "axes.spines.top": False, "axes.spines.right": False,
    "axes.titleweight": "bold", "axes.titlesize": 12, "axes.grid": True, "grid.alpha": 0.25,
    "font.size": 10,
})
