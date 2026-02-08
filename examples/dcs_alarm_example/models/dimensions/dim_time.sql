{{ config "materialized" "table" }}

-- Time Dimension with 10-minute buckets
-- Pre-computed time buckets for optimized time-based queries (144 buckets per day)

WITH time_buckets AS (
  SELECT
    CAST(column1 AS INTEGER) AS time_key
  FROM (
    VALUES
      (0), (1), (2), (3), (4), (5), (6), (7), (8), (9),
      (10), (11), (12), (13), (14), (15), (16), (17), (18), (19),
      (20), (21), (22), (23), (24), (25), (26), (27), (28), (29),
      (30), (31), (32), (33), (34), (35), (36), (37), (38), (39),
      (40), (41), (42), (43), (44), (45), (46), (47), (48), (49),
      (50), (51), (52), (53), (54), (55), (56), (57), (58), (59),
      (60), (61), (62), (63), (64), (65), (66), (67), (68), (69),
      (70), (71), (72), (73), (74), (75), (76), (77), (78), (79),
      (80), (81), (82), (83), (84), (85), (86), (87), (88), (89),
      (90), (91), (92), (93), (94), (95), (96), (97), (98), (99),
      (100), (101), (102), (103), (104), (105), (106), (107), (108), (109),
      (110), (111), (112), (113), (114), (115), (116), (117), (118), (119),
      (120), (121), (122), (123), (124), (125), (126), (127), (128), (129),
      (130), (131), (132), (133), (134), (135), (136), (137), (138), (139),
      (140), (141), (142), (143)
  )
)
SELECT
  time_key,
  time_key AS time_bucket_10min,
  time_key / 6 AS hour,
  (time_key % 6) * 10 AS minute_start,
  printf('%02d:%02d', time_key / 6, (time_key % 6) * 10) AS time_display,
  CASE
    WHEN time_key BETWEEN 0 AND 47 THEN 'A'
    WHEN time_key BETWEEN 48 AND 95 THEN 'B'
    ELSE 'C'
  END AS shift
FROM time_buckets
ORDER BY time_key
