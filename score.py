import iditarod as idt


def mean(li):
  return float(sum(li))/len(li)

if __name__ == "__main__":
  engine = idt.loadEngine(idt.DB_LOCATION)
  session = idt.loadSession(engine)
  results = []
  racers_agg = {}
  for race in [2015+i for i in range(3)]:
    results.append(idt.io_score_race(session, race))

  for result in results:
    for racer in result:
      if racer not in racers_agg:
        racers_agg[racer] = [result[racer]]
      else:
        racers_agg[racer].append(result[racer])
  print(racers_agg)

  mean_score = [(racer, mean(racers_agg[racer]), racers_agg[racer]) for racer in racers_agg]
  mean_score.sort(key=lambda x: x[1])

  for i in mean_score:
    print(i)



