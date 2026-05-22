"""
Parse GLM-5 token billing data and calculate totals.
"""

import re
from collections import defaultdict

# Billing data pasted from user
RAW_DATA = """
2026-04-24 17:19:13 glm-5;context_32k-200k_input_token_cache;1 2.200290 十万tokens 26.403480 Credits
2026-04-24 17:19:12 glm-5;context_32k-200k_input_token;1 5.408000 千tokens 3.244800 Credits
2026-04-24 17:19:12 glm-5;context_32k-200k_output_token;1 2.677000 千tokens 5.889400 Credits
2026-04-24 17:18:11 glm-5;context_32k-200k_input_token_cache;1 2.751960 十万tokens 33.023520 Credits
2026-04-24 17:18:11 glm-5;context_32k-200k_output_token;1 1.956000 千tokens 4.303200 Credits
2026-04-24 17:18:10 glm-5;context_32k-200k_input_token;1 3.854000 千tokens 2.312400 Credits
2026-04-24 17:17:12 glm-5;context_32k-200k_output_token;1 1.933000 千tokens 4.252600 Credits
2026-04-24 17:17:11 glm-5;context_32k-200k_input_token_cache;1 1.254380 十万tokens 15.052560 Credits
2026-04-24 17:17:11 glm-5;context_32k-200k_input_token;1 8.419000 千tokens 5.051400 Credits
2026-04-24 17:15:12 glm-5;context_32k-200k_input_token;1 3.937000 千tokens 2.362200 Credits
2026-04-24 17:15:12 glm-5;context_32k-200k_output_token;1 399.000000 tokens 0.877800 Credits
2026-04-24 17:15:12 glm-5;context_32k-200k_input_token_cache;1 6.233500 万tokens 7.480200 Credits
2026-04-24 17:14:12 glm-5;context_32k-200k_input_token;1 7.260000 千tokens 4.356000 Credits
2026-04-24 17:14:12 glm-5;context_32k-200k_input_token_cache;1 1.146860 十万tokens 13.762320 Credits
2026-04-24 17:14:12 glm-5;context_32k-200k_output_token;1 2.839000 千tokens 6.245800 Credits
2026-04-24 17:12:12 glm-5;context_32k-200k_input_token_cache;1 5.663900 万tokens 6.796680 Credits
2026-04-24 17:12:12 glm-5;context_32k-200k_input_token;1 2.736000 千tokens 1.641600 Credits
2026-04-24 17:12:11 glm-5;context_32k-200k_output_token;1 1.453000 千tokens 3.196600 Credits
2026-04-24 17:11:11 glm-5;context_32k-200k_input_token;1 1.867500 万tokens 11.205000 Credits
2026-04-24 17:11:11 glm-5;context_32k-200k_input_token_cache;1 9.318200 万tokens 11.181840 Credits
2026-04-24 17:11:11 glm-5;context_32k-200k_output_token;1 1.557000 千tokens 3.425400 Credits
2026-04-24 17:05:14 glm-5;context_32k-200k_output_token;1 509.000000 tokens 1.119800 Credits
2026-04-24 17:05:14 glm-5;context_32k-200k_input_token_cache;1 5.324700 万tokens 6.389640 Credits
2026-04-24 17:05:14 glm-5;context_32k-200k_input_token;1 1.761000 千tokens 1.056600 Credits
2026-04-24 17:04:13 glm-5;context_32k-200k_output_token;1 3.394000 千tokens 7.466800 Credits
2026-04-24 17:04:13 glm-5;context_32k-200k_input_token;1 4.133100 万tokens 24.798600 Credits
2026-04-24 17:04:13 glm-5;context_32k-200k_input_token_cache;1 1.671000 十万tokens 20.052000 Credits
2026-04-24 17:03:12 glm-5;context_32k-200k_input_token_cache;1 4.825500 万tokens 5.790600 Credits
2026-04-24 17:03:11 glm-5;context_32k-200k_input_token;1 1.794000 千tokens 1.076400 Credits
2026-04-24 17:03:11 glm-5;context_32k-200k_output_token;1 1.186000 千tokens 2.609200 Credits
2026-04-24 17:02:11 glm-5;context_32k-200k_input_token;1 1.343800 万tokens 8.062800 Credits
2026-04-24 17:02:11 glm-5;context_32k-200k_output_token;1 2.074000 千tokens 4.562800 Credits
2026-04-24 17:02:11 glm-5;context_32k-200k_input_token_cache;1 2.191310 十万tokens 26.295720 Credits
2026-04-24 17:01:12 glm-5;context_32k-200k_input_token_cache;1 1.179480 十万tokens 14.153760 Credits
2026-04-24 17:01:11 glm-5;context_32k-200k_output_token;1 1.937000 千tokens 4.261400 Credits
2026-04-24 17:01:11 glm-5;context_32k-200k_input_token;1 5.680900 万tokens 34.085400 Credits
2026-04-24 17:00:15 glm-5;context_32k-200k_output_token;1 1.461000 千tokens 3.214200 Credits
2026-04-24 17:00:14 glm-5;context_32k-200k_input_token_cache;1 1.974350 十万tokens 23.692200 Credits
2026-04-24 17:00:14 glm-5;context_32k-200k_input_token;1 3.212000 千tokens 1.927200 Credits
2026-04-24 16:59:13 glm-5;context_32k-200k_input_token_cache;1 1.592910 十万tokens 19.114920 Credits
2026-04-24 16:59:13 glm-5;context_32k-200k_input_token;1 2.666200 万tokens 15.997200 Credits
2026-04-24 16:59:13 glm-5;context_32k-200k_output_token;1 1.329000 千tokens 2.923800 Credits
2026-04-24 16:58:14 glm-5;context_0-32k_input_token;1 1.581300 万tokens 6.325200 Credits
2026-04-24 16:58:13 glm-5;context_32k-200k_output_token;1 2.563000 千tokens 5.638600 Credits
2026-04-24 16:58:13 glm-5;context_0-32k_input_token_cache;1 1.663900 万tokens 1.331120 Credits
2026-04-24 16:58:12 glm-5;context_0-32k_output_token;1 189.000000 tokens 0.340200 Credits
2026-04-24 16:58:12 glm-5;context_32k-200k_input_token;1 1.390200 万tokens 8.341200 Credits
2026-04-24 16:58:12 glm-5;context_32k-200k_input_token_cache;1 1.578870 十万tokens 18.946440 Credits
2026-04-24 16:57:13 glm-5;context_32k-200k_output_token;1 3.416000 千tokens 7.515200 Credits
2026-04-24 16:57:13 glm-5;context_32k-200k_input_token_cache;1 1.617910 十万tokens 19.414920 Credits
2026-04-24 16:57:12 glm-5;context_32k-200k_input_token;1 5.647000 千tokens 3.388200 Credits
2026-04-24 16:55:12 glm-5;context_32k-200k_output_token;1 3.913000 千tokens 8.608600 Credits
2026-04-24 16:55:12 glm-5;context_32k-200k_input_token_cache;1 2.534200 万tokens 3.041040 Credits
2026-04-24 16:55:12 glm-5;context_32k-200k_input_token;1 2.943870 十万tokens 176.632200 Credits
2026-04-24 16:15:12 glm-5;context_32k-200k_input_token;1 2.766000 千tokens 1.659600 Credits
2026-04-24 16:15:12 glm-5;context_32k-200k_input_token_cache;1 1.550070 十万tokens 18.600840 Credits
2026-04-24 16:15:12 glm-5;context_32k-200k_output_token;1 525.000000 tokens 1.155000 Credits
2026-04-24 16:14:13 glm-5;context_32k-200k_output_token;1 2.718000 千tokens 5.979600 Credits
2026-04-24 16:14:12 glm-5;context_32k-200k_input_token;1 4.104000 千tokens 2.462400 Credits
2026-04-24 16:14:12 glm-5;context_32k-200k_input_token_cache;1 1.509110 十万tokens 18.109320 Credits
2026-04-24 16:13:14 glm-5;context_32k-200k_output_token;1 634.000000 tokens 1.394800 Credits
2026-04-24 16:13:14 glm-5;context_32k-200k_input_token;1 1.492890 十万tokens 89.573400 Credits
2026-04-24 16:13:14 glm-5;context_32k-200k_input_token_cache;1 2.891490 十万tokens 34.697880 Credits
2026-04-24 16:12:11 glm-5;context_32k-200k_output_token;1 1.808000 千tokens 3.977600 Credits
2026-04-24 16:12:11 glm-5;context_32k-200k_input_token;1 1.395830 十万tokens 83.749800 Credits
2026-04-24 16:12:11 glm-5;context_32k-200k_input_token_cache;1 1.727000 千tokens 0.207240 Credits
2026-04-24 15:55:12 glm-5;context_32k-200k_input_token;1 1.284040 十万tokens 77.042400 Credits
2026-04-24 15:55:12 glm-5;context_32k-200k_input_token_cache;1 1.535340 十万tokens 18.424080 Credits
2026-04-24 15:55:12 glm-5;context_32k-200k_output_token;1 948.000000 tokens 2.085600 Credits
2026-04-24 15:54:13 glm-5;context_32k-200k_input_token;1 1.222760 十万tokens 73.365600 Credits
2026-04-24 15:54:13 glm-5;context_32k-200k_input_token_cache;1 1.267100 万tokens 1.520520 Credits
2026-04-24 15:54:13 glm-5;context_32k-200k_output_token;1 5.880000 千tokens 12.936000 Credits
2026-04-24 15:30:12 glm-5;context_32k-200k_output_token;1 514.000000 tokens 1.130800 Credits
2026-04-24 15:30:12 glm-5;context_32k-200k_input_token_cache;1 1.354230 十万tokens 16.250760 Credits
2026-04-24 15:30:12 glm-5;context_32k-200k_input_token;1 216.000000 tokens 0.129600 Credits
2026-04-24 15:29:11 glm-5;context_32k-200k_input_token;1 4.493000 千tokens 2.695800 Credits
2026-04-24 15:29:11 glm-5;context_32k-200k_input_token_cache;1 2.638060 十万tokens 31.656720 Credits
2026-04-24 15:29:10 glm-5;context_32k-200k_output_token;1 2.747000 千tokens 6.043400 Credits
2026-04-24 15:28:12 glm-5;context_32k-200k_output_token;1 390.000000 tokens 0.858000 Credits
2026-04-24 15:28:11 glm-5;context_32k-200k_input_token;1 1.183770 十万tokens 71.026200 Credits
2026-04-24 15:28:11 glm-5;context_32k-200k_input_token_cache;1 1.267100 万tokens 1.520520 Credits
2026-04-24 15:27:10 glm-5;context_32k-200k_output_token;1 1.184000 千tokens 2.604800 Credits
2026-04-24 15:27:09 glm-5;context_32k-200k_input_token_cache;1 4.935640 十万tokens 59.227680 Credits
2026-04-24 15:27:09 glm-5;context_32k-200k_input_token;1 4.043000 千tokens 2.425800 Credits
2026-04-24 15:26:10 glm-5;context_32k-200k_input_token_cache;1 1.964140 十万tokens 23.569680 Credits
2026-04-24 15:26:10 glm-5;context_32k-200k_output_token;1 502.000000 tokens 1.104400 Credits
2026-04-24 15:26:09 glm-5;context_32k-200k_input_token;1 4.595000 万tokens 27.570000 Credits
2026-04-24 15:19:13 glm-5;context_0-32k_input_token_cache;1 1.273400 万tokens 1.018720 Credits
2026-04-24 15:19:13 glm-5;context_0-32k_input_token;1 1.439600 万tokens 5.758400 Credits
2026-04-24 15:19:13 glm-5;context_0-32k_output_token;1 606.000000 tokens 1.090800 Credits
2026-04-24 15:16:14 glm-5;context_32k-200k_output_token;1 1.294400 万tokens 28.476800 Credits
2026-04-24 15:16:14 glm-5;context_32k-200k_input_token;1 1.353600 万tokens 8.121600 Credits
2026-04-24 15:16:14 glm-5;context_32k-200k_input_token_cache;1 3.363810 十万tokens 40.365720 Credits
2026-04-24 15:14:12 glm-5;context_32k-200k_input_token;1 270.000000 tokens 0.162000 Credits
2026-04-24 15:14:12 glm-5;context_32k-200k_output_token;1 1.924100 万tokens 42.330200 Credits
2026-04-24 15:14:11 glm-5;context_32k-200k_input_token_cache;1 7.539100 万tokens 9.046920 Credits
2026-04-24 15:11:12 glm-5;context_32k-200k_input_token_cache;1 4.682180 十万tokens 56.186160 Credits
2026-04-24 15:11:12 glm-5;context_32k-200k_output_token;1 931.000000 tokens 2.048200 Credits
2026-04-24 15:11:12 glm-5;context_32k-200k_input_token;1 9.796600 万tokens 58.779600 Credits
2026-04-24 15:10:14 glm-5;context_32k-200k_input_token;1 5.267000 千tokens 3.160200 Credits
2026-04-24 15:10:14 glm-5;context_32k-200k_output_token;1 1.564000 千tokens 3.440800 Credits
2026-04-24 15:10:13 glm-5;context_32k-200k_input_token_cache;1 3.231320 十万tokens 38.775840 Credits
2026-04-24 15:09:13 glm-5;context_32k-200k_output_token;1 1.502000 千tokens 3.304400 Credits
2026-04-24 15:09:13 glm-5;context_32k-200k_input_token_cache;1 3.046360 十万tokens 36.556320 Credits
2026-04-24 15:09:13 glm-5;context_0-32k_input_token;1 238.000000 tokens 0.095200 Credits
2026-04-24 15:09:13 glm-5;context_32k-200k_input_token;1 3.270000 千tokens 1.962000 Credits
2026-04-24 15:09:12 glm-5;context_0-32k_output_token;1 214.000000 tokens 0.385200 Credits
2026-04-24 15:08:14 glm-5;context_32k-200k_input_token_cache;1 1.267100 万tokens 1.520520 Credits
2026-04-24 15:08:14 glm-5;context_0-32k_input_token;1 338.000000 tokens 0.135200 Credits
2026-04-24 15:08:13 glm-5;context_32k-200k_input_token;1 6.276500 万tokens 37.659000 Credits
2026-04-24 15:08:13 glm-5;context_32k-200k_output_token;1 191.000000 tokens 0.420200 Credits
2026-04-24 15:08:13 glm-5;context_0-32k_output_token;1 147.000000 tokens 0.264600 Credits
2026-04-24 15:05:13 glm-5;context_32k-200k_output_token;1 6.545000 千tokens 14.399000 Credits
2026-04-24 15:05:12 glm-5;context_32k-200k_input_token_cache;1 1.845080 十万tokens 22.140960 Credits
2026-04-24 15:05:12 glm-5;context_32k-200k_input_token;1 1.121620 十万tokens 67.297200 Credits
2026-04-24 15:03:13 glm-5;context_32k-200k_input_token;1 3.621400 万tokens 21.728400 Credits
2026-04-24 15:03:13 glm-5;context_32k-200k_input_token_cache;1 1.663900 万tokens 1.996680 Credits
2026-04-24 15:03:13 glm-5;context_32k-200k_output_token;1 742.000000 tokens 1.632400 Credits
2026-04-24 15:02:12 glm-5;context_0-32k_input_token_cache;1 3.321400 万tokens 2.657120 Credits
2026-04-24 15:02:12 glm-5;context_0-32k_output_token;1 488.000000 tokens 0.878400 Credits
2026-04-24 15:02:12 glm-5;context_0-32k_input_token;1 3.221200 万tokens 12.884800 Credits
2026-04-24 14:51:12 glm-5;context_0-32k_input_token;1 703.000000 tokens 0.281200 Credits
2026-04-24 14:51:12 glm-5;context_0-32k_output_token;1 200.000000 tokens 0.360000 Credits
2026-04-24 14:51:12 glm-5;context_0-32k_input_token_cache;1 3.199900 万tokens 2.559920 Credits
2026-04-24 14:50:12 glm-5;context_0-32k_input_token;1 5.041400 万tokens 20.165600 Credits
2026-04-24 14:50:12 glm-5;context_0-32k_input_token_cache;1 4.999700 万tokens 3.999760 Credits
2026-04-24 14:50:11 glm-5;context_0-32k_output_token;1 2.551000 千tokens 4.591800 Credits
2026-04-24 14:49:10 glm-5;context_0-32k_input_token;1 156.000000 tokens 0.062400 Credits
2026-04-24 14:49:10 glm-5;context_0-32k_output_token;1 93.000000 tokens 0.167400 Credits
2026-04-24 14:34:12 glm-5;context_32k-200k_input_token;1 3.663000 千tokens 2.197800 Credits
2026-04-24 14:34:11 glm-5;context_32k-200k_output_token;1 3.007000 千tokens 6.615400 Credits
2026-04-24 14:34:11 glm-5;context_32k-200k_input_token_cache;1 2.958060 十万tokens 35.496720 Credits
2026-04-24 14:32:13 glm-5;context_32k-200k_output_token;1 771.000000 tokens 1.696200 Credits
2026-04-24 14:32:12 glm-5;context_32k-200k_input_token;1 1.768600 万tokens 10.611600 Credits
2026-04-24 14:32:11 glm-5;context_32k-200k_input_token_cache;1 1.278007 百万tokens 153.360840 Credits
2026-04-24 14:31:12 glm-5;context_32k-200k_output_token;1 572.000000 tokens 1.258400 Credits
2026-04-24 14:31:12 glm-5;context_32k-200k_input_token;1 1.066900 十万tokens 64.014000 Credits
2026-04-24 14:30:11 glm-5;context_0-32k_input_token;1 7.827000 千tokens 3.130800 Credits
2026-04-24 14:30:11 glm-5;context_0-32k_output_token;1 523.000000 tokens 0.941400 Credits
2026-04-24 14:30:11 glm-5;context_0-32k_input_token_cache;1 1.740000 千tokens 0.139200 Credits
2026-04-24 14:29:12 glm-5;context_32k-200k_output_token;1 368.000000 tokens 0.809600 Credits
2026-04-24 14:29:12 glm-5;context_32k-200k_input_token;1 583.000000 tokens 0.349800 Credits
2026-04-24 14:29:12 glm-5;context_32k-200k_input_token_cache;1 1.305590 十万tokens 15.667080 Credits
2026-04-24 14:28:12 glm-5;context_32k-200k_output_token;1 141.000000 tokens 0.310200 Credits
2026-04-24 14:28:12 glm-5;context_32k-200k_input_token;1 1.139210 十万tokens 68.352600 Credits
2026-04-24 14:28:11 glm-5;context_32k-200k_input_token_cache;1 1.663900 万tokens 1.996680 Credits
2026-04-24 14:27:13 glm-5;context_32k-200k_input_token;1 1.134250 十万tokens 68.055000 Credits
2026-04-24 14:27:13 glm-5;context_32k-200k_output_token;1 4.369000 千tokens 9.611800 Credits
2026-04-24 14:27:12 glm-5;context_32k-200k_input_token_cache;1 1.393900 十万tokens 16.726800 Credits
2026-04-24 14:24:13 glm-5;context_32k-200k_output_token;1 597.000000 tokens 1.313400 Credits
2026-04-24 14:24:12 glm-5;context_32k-200k_input_token_cache;1 4.796120 十万tokens 57.553440 Credits
2026-04-24 14:24:11 glm-5;context_32k-200k_input_token;1 4.953000 千tokens 2.971800 Credits
2026-04-24 14:23:13 glm-5;context_32k-200k_output_token;1 950.000000 tokens 2.090000 Credits
2026-04-24 14:23:13 glm-5;context_32k-200k_input_token;1 2.960000 千tokens 1.776000 Credits
2026-04-24 14:23:12 glm-5;context_32k-200k_input_token_cache;1 2.312940 十万tokens 27.755280 Credits
2026-04-24 14:22:14 glm-5;context_32k-200k_input_token;1 8.370000 千tokens 5.022000 Credits
2026-04-24 14:22:13 glm-5;context_32k-200k_input_token_cache;1 1.066870 十万tokens 12.802440 Credits
2026-04-24 14:22:13 glm-5;context_32k-200k_output_token;1 715.000000 tokens 1.573000 Credits
2026-04-24 14:21:14 glm-5;context_32k-200k_output_token;1 45.000000 tokens 0.099000 Credits
2026-04-24 14:21:14 glm-5;context_32k-200k_input_token_cache;1 1.663900 万tokens 1.996680 Credits
2026-04-24 14:21:13 glm-5;context_32k-200k_input_token;1 9.010600 万tokens 54.063600 Credits
2026-04-24 14:18:12 glm-5;context_32k-200k_input_token;1 8.911100 万tokens 53.466600 Credits
2026-04-24 14:18:12 glm-5;context_32k-200k_output_token;1 910.000000 tokens 2.002000 Credits
2026-04-24 14:18:11 glm-5;context_32k-200k_input_token_cache;1 1.663900 万tokens 1.996680 Credits
2026-04-24 13:54:13 glm-5;context_32k-200k_output_token;1 3.816000 千tokens 8.395200 Credits
2026-04-24 13:54:13 glm-5;context_32k-200k_input_token_cache;1 2.008940 十万tokens 24.107280 Credits
2026-04-24 13:54:13 glm-5;context_32k-200k_input_token;1 6.246000 千tokens 3.747600 Credits
2026-04-24 13:49:11 glm-5;context_32k-200k_output_token;1 556.000000 tokens 1.223200 Credits
2026-04-24 13:49:11 glm-5;context_32k-200k_input_token;1 770.000000 tokens 0.462000 Credits
2026-04-24 13:49:11 glm-5;context_32k-200k_input_token_cache;1 9.907100 万tokens 11.888520 Credits
2026-04-24 13:48:12 glm-5;context_32k-200k_input_token_cache;1 1.832300 十万tokens 21.987600 Credits
2026-04-24 13:48:12 glm-5;context_32k-200k_input_token;1 1.566300 万tokens 9.397800 Credits
2026-04-24 13:48:12 glm-5;context_32k-200k_output_token;1 864.000000 tokens 1.900800 Credits
2026-04-24 13:44:11 glm-5;context_32k-200k_input_token;1 7.643500 万tokens 45.861000 Credits
2026-04-24 13:44:11 glm-5;context_32k-200k_output_token;1 3.189000 千tokens 7.015800 Credits
2026-04-24 13:44:11 glm-5;context_32k-200k_input_token_cache;1 1.188460 十万tokens 14.261520 Credits
2026-04-24 13:42:10 glm-5;context_32k-200k_output_token;1 421.000000 tokens 0.926200 Credits
2026-04-24 13:42:10 glm-5;context_32k-200k_input_token_cache;1 1.657500 万tokens 1.989000 Credits
2026-04-24 13:42:09 glm-5;context_32k-200k_input_token;1 6.764500 万tokens 40.587000 Credits
2026-04-24 13:39:11 glm-5;context_32k-200k_output_token;1 5.249000 千tokens 11.547800 Credits
2026-04-24 13:39:11 glm-5;context_32k-200k_input_token_cache;1 1.307510 十万tokens 15.690120 Credits
2026-04-24 13:39:11 glm-5;context_32k-200k_input_token;1 7.603000 千tokens 4.561800 Credits
2026-04-24 13:37:11 glm-5;context_32k-200k_input_token;1 1.141220 十万tokens 68.473200 Credits
2026-04-24 13:37:10 glm-5;context_32k-200k_input_token_cache;1 1.663900 万tokens 1.996680 Credits
2026-04-24 13:37:10 glm-5;context_32k-200k_output_token;1 548.000000 tokens 1.205600 Credits
2026-04-24 13:25:11 glm-5;context_32k-200k_output_token;1 1.715000 千tokens 3.773000 Credits
2026-04-24 13:25:10 glm-5;context_32k-200k_input_token_cache;1 1.663900 万tokens 1.996680 Credits
2026-04-24 13:25:10 glm-5;context_32k-200k_input_token;1 1.147820 十万tokens 68.869200 Credits
2026-04-24 13:24:11 glm-5;context_32k-200k_input_token_cache;1 1.247990 十万tokens 14.975880 Credits
2026-04-24 13:24:11 glm-5;context_32k-200k_output_token;1 2.623000 千tokens 5.770600 Credits
2026-04-24 13:24:11 glm-5;context_32k-200k_input_token;1 2.436000 千tokens 1.461600 Credits
2026-04-24 13:22:12 glm-5;context_32k-200k_output_token;1 1.999000 千tokens 4.397800 Credits
2026-04-24 13:22:11 glm-5;context_32k-200k_input_token;1 2.796000 千tokens 1.677600 Credits
2026-04-24 13:22:11 glm-5;context_32k-200k_input_token_cache;1 1.220470 十万tokens 14.645640 Credits
2026-04-24 13:21:11 glm-5;context_32k-200k_input_token_cache;1 1.213430 十万tokens 14.561160 Credits
2026-04-24 13:21:11 glm-5;context_32k-200k_input_token;1 765.000000 tokens 0.459000 Credits
2026-04-24 13:21:10 glm-5;context_32k-200k_output_token;1 1.583000 千tokens 3.482600 Credits
2026-04-24 13:20:11 glm-5;context_32k-200k_input_token;1 6.872000 千tokens 4.123200 Credits
2026-04-24 13:20:11 glm-5;context_32k-200k_output_token;1 1.535000 千tokens 3.377000 Credits
2026-04-24 13:20:11 glm-5;context_32k-200k_input_token_cache;1 2.344300 十万tokens 28.131600 Credits
2026-04-24 13:19:12 glm-5;context_0-32k_output_token;1 736.000000 tokens 1.324800 Credits
2026-04-24 13:19:12 glm-5;context_0-32k_input_token;1 2.555800 万tokens 10.223200 Credits
2026-04-24 13:19:12 glm-5;context_32k-200k_output_token;1 5.397000 千tokens 11.873400 Credits
2026-04-24 13:19:12 glm-5;context_32k-200k_input_token_cache;1 1.096310 十万tokens 13.155720 Credits
2026-04-24 13:19:12 glm-5;context_32k-200k_input_token;1 4.875000 千tokens 2.925000 Credits
2026-04-24 13:19:12 glm-5;context_0-32k_input_token_cache;1 96.000000 tokens 0.007680 Credits
2026-04-24 13:17:11 glm-5;context_32k-200k_output_token;1 1.550000 千tokens 3.410000 Credits
2026-04-24 13:17:11 glm-5;context_32k-200k_input_token_cache;1 1.050230 十万tokens 12.602760 Credits
2026-04-24 13:17:11 glm-5;context_32k-200k_input_token;1 4.672000 千tokens 2.803200 Credits
2026-04-24 13:16:11 glm-5;context_32k-200k_input_token;1 8.847600 万tokens 53.085600 Credits
"""

def parse_token_value(value_str):
    """Parse token count string into integer."""
    value_str = value_str.strip()

    # Match patterns for different units
    patterns = [
        (r'([\d.]+)\s*百万tokens', 1_000_000),  # million tokens
        (r'([\d.]+)\s*十万tokens', 100_000),    # 100k tokens
        (r'([\d.]+)\s*万tokens', 10_000),       # 10k tokens
        (r'([\d.]+)\s*千tokens', 1_000),        # 1k tokens
        (r'([\d.]+)\s*tokens', 1),              # single tokens
    ]

    for pattern, multiplier in patterns:
        match = re.search(pattern, value_str)
        if match:
            num = float(match.group(1))
            return int(num * multiplier)
    return 0

def parse_credits(credits_str):
    """Parse credits string into float."""
    match = re.search(r'([\d.]+)\s*Credits', credits_str)
    if match:
        return float(match.group(1))
    return 0

def parse_records():
    """Parse all billing records."""
    records = []

    for line in RAW_DATA.strip().split('\n'):
        parts = line.strip().split()

        if len(parts) < 6:
            continue

        timestamp = parts[0] + ' ' + parts[1]
        token_type = parts[2]
        usage_str = parts[3] + ' ' + parts[4]
        credits_str = parts[5] + ' ' + parts[6] if len(parts) > 6 else parts[5]

        tokens = parse_token_value(usage_str)
        credits = parse_credits(credits_str)

        if tokens > 0 or credits > 0:
            records.append({
                'timestamp': timestamp,
                'token_type': token_type,
                'tokens': tokens,
                'credits': credits
            })

    return records

def main():
    records = parse_records()

    # Summarize by token type
    summary = defaultdict(lambda: {'tokens': 0, 'credits': 0})
    for rec in records:
        key = rec['token_type']
        summary[key]['tokens'] += rec['tokens']
        summary[key]['credits'] += rec['credits']

    # Group into categories
    categories = {
        'input_32k': defaultdict(lambda: {'tokens': 0, 'credits': 0}),
        'cache_32k': defaultdict(lambda: {'tokens': 0, 'credits': 0}),
        'output_32k': defaultdict(lambda: {'tokens': 0, 'credits': 0}),
        'input_0_32k': defaultdict(lambda: {'tokens': 0, 'credits': 0}),
        'cache_0_32k': defaultdict(lambda: {'tokens': 0, 'credits': 0}),
        'output_0_32k': defaultdict(lambda: {'tokens': 0, 'credits': 0}),
    }

    total_tokens = 0
    total_credits = 0

    for key, data in summary.items():
        if 'context_32k-200k_input_token_cache' in key:
            categories['cache_32k']['tokens'] = data['tokens']
            categories['cache_32k']['credits'] = data['credits']
        elif 'context_32k-200k_input_token' in key:
            categories['input_32k']['tokens'] = data['tokens']
            categories['input_32k']['credits'] = data['credits']
        elif 'context_32k-200k_output_token' in key:
            categories['output_32k']['tokens'] = data['tokens']
            categories['output_32k']['credits'] = data['credits']
        elif 'context_0-32k_input_token_cache' in key:
            categories['cache_0_32k']['tokens'] = data['tokens']
            categories['cache_0_32k']['credits'] = data['credits']
        elif 'context_0-32k_input_token' in key:
            categories['input_0_32k']['tokens'] = data['tokens']
            categories['input_0_32k']['credits'] = data['credits']
        elif 'context_0-32k_output_token' in key:
            categories['output_0_32k']['tokens'] = data['tokens']
            categories['output_0_32k']['credits'] = data['credits']

        total_tokens += data['tokens']
        total_credits += data['credits']

    # Print summary
    print("=" * 70)
    print("GLM-5 Token Usage Summary (2026-04-24)")
    print("=" * 70)

    print("\n### Large Context (32k-200k tokens)")
    print("-" * 50)
    print(f"Input Tokens:        {categories['input_32k']['tokens']:>12,} tokens")
    print(f"Input Token Cost:    {categories['input_32k']['credits']:>12.2f} Credits")
    print(f"Cached Input:        {categories['cache_32k']['tokens']:>12,} tokens")
    print(f"Cache Cost:          {categories['cache_32k']['credits']:>12.2f} Credits")
    print(f"Output Tokens:       {categories['output_32k']['tokens']:>12,} tokens")
    print(f"Output Cost:         {categories['output_32k']['credits']:>12.2f} Credits")

    print("\n### Standard Context (0-32k tokens)")
    print("-" * 50)
    print(f"Input Tokens:        {categories['input_0_32k']['tokens']:>12,} tokens")
    print(f"Input Token Cost:    {categories['input_0_32k']['credits']:>12.2f} Credits")
    print(f"Cached Input:        {categories['cache_0_32k']['tokens']:>12,} tokens")
    print(f"Cache Cost:          {categories['cache_0_32k']['credits']:>12.2f} Credits")
    print(f"Output Tokens:       {categories['output_0_32k']['tokens']:>12,} tokens")
    print(f"Output Cost:         {categories['output_0_32k']['credits']:>12.2f} Credits")

    print("\n### Totals")
    print("=" * 70)

    total_input = categories['input_32k']['tokens'] + categories['input_0_32k']['tokens']
    total_cache = categories['cache_32k']['tokens'] + categories['cache_0_32k']['tokens']
    total_output = categories['output_32k']['tokens'] + categories['output_0_32k']['tokens']
    total_input_cost = categories['input_32k']['credits'] + categories['input_0_32k']['credits']
    total_cache_cost = categories['cache_32k']['credits'] + categories['cache_0_32k']['credits']
    total_output_cost = categories['output_32k']['credits'] + categories['output_0_32k']['credits']

    print(f"Total Input Tokens:      {total_input:>14,} tokens")
    print(f"Total Cached Input:      {total_cache:>14,} tokens")
    print(f"Total Output Tokens:     {total_output:>14,} tokens")
    print(f"Total All Tokens:        {total_tokens:>14,} tokens")
    print()
    print(f"Total Input Cost:        {total_input_cost:>14.2f} Credits")
    print(f"Total Cache Cost:        {total_cache_cost:>14.2f} Credits")
    print(f"Total Output Cost:       {total_output_cost:>14.2f} Credits")
    print(f"Total Cost:              {total_credits:>14.2f} Credits")

    # Calculate effective rates
    print("\n### Effective Rates Observed")
    print("-" * 50)
    if categories['input_32k']['tokens'] > 0:
        rate = categories['input_32k']['credits'] / categories['input_32k']['tokens']
        print(f"Input (32k-200k):        {rate:.6f} Credits/token (0.0006)")
    if categories['cache_32k']['tokens'] > 0:
        rate = categories['cache_32k']['credits'] / categories['cache_32k']['tokens']
        print(f"Cache (32k-200k):        {rate:.6f} Credits/token (0.00012)")
    if categories['output_32k']['tokens'] > 0:
        rate = categories['output_32k']['credits'] / categories['output_32k']['tokens']
        print(f"Output (32k-200k):       {rate:.6f} Credits/token (0.0022)")
    if categories['input_0_32k']['tokens'] > 0:
        rate = categories['input_0_32k']['credits'] / categories['input_0_32k']['tokens']
        print(f"Input (0-32k):           {rate:.6f} Credits/token (0.0004)")
    if categories['cache_0_32k']['tokens'] > 0:
        rate = categories['cache_0_32k']['credits'] / categories['cache_0_32k']['tokens']
        print(f"Cache (0-32k):           {rate:.6f} Credits/token (0.00008)")
    if categories['output_0_32k']['tokens'] > 0:
        rate = categories['output_0_32k']['credits'] / categories['output_0_32k']['tokens']
        print(f"Output (0-32k):          {rate:.6f} Credits/token (0.0018)")

    # Cache savings
    print("\n### Prompt Cache Savings Analysis")
    print("-" * 50)
    if total_cache > 0:
        # Without cache: would pay input rate
        saved_32k = categories['cache_32k']['tokens'] * 0.0006 - categories['cache_32k']['credits']
        saved_0_32k = categories['cache_0_32k']['tokens'] * 0.0004 - categories['cache_0_32k']['credits']
        total_saved = saved_32k + saved_0_32k
        print(f"32k-200k cache saved:    {saved_32k:>12.2f} Credits")
        print(f"0-32k cache saved:       {saved_0_32k:>12.2f} Credits")
        print(f"Total cache savings:     {total_saved:>12.2f} Credits")

        cache_ratio = total_cache / (total_input + total_cache) * 100
        print(f"Cache hit ratio:         {cache_ratio:>12.1f}% of prompts")

if __name__ == '__main__':
    main()