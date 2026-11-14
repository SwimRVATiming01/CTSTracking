# Companion: consolidating Next Event / Next Heat actions

Status: **recommendation only, not implemented** (2026-11-08)
Source: Companion export `CSAC-014_2026-11-08-1806_custom_config.companionconfig`
(Companion 4.3.2), CTS page (page 3).

## Current setup

### Switcher button 3/2/6 (working, keep as is)
Both the feedback ("Use another button's style", `bank_style`) and the press action
(`button_pressrelease`) use this location expression:

```
$(custom:cts_next_action) =='NEXT EVENT' ? '3/2/7' : '3/3/7'
```

- `cts_next_action` == `NEXT EVENT`: shows and presses 3/2/7 (Next Event).
- Anything else, including empty: shows and presses 3/3/7 (Next Heat).
- `cts_next_action` starts empty and doesn't persist, so after a Companion
  restart the switcher defaults to **Next Heat**. That's fine for now;
  Next Heat vs Next Event as the default is still undecided.
- Location strings point at grid positions. If 3/2/7 or 3/3/7 moves,
  update the expression.

### What 3/2/7 and 3/3/7 do

| Action | 3/2/7 Next Event | 3/3/7 Next Heat |
|---|---|---|
| Local `Reset` = 1 (flash feedback, `$(this:location)`) | yes | yes |
| POST `/api/companion/lanes?s=$(custom:LaneIOString)` to CTSTracking | yes | yes |
| SpotifyPC Ctrl+F13 | yes | yes |
| Dolphin5TCP send `reset` | yes | yes |
| Wait 10 s, reset local `Reset`, reset `LaneIOString` | yes | yes |
| **GEN7 special key** | **F14** | **F13** |
| **Dolphin5Vicreo special key** | **F14** | **F15** |
| **exec: append timestamp to C:\Log\timestamps.txt** | no | **yes** |

About 80% of each button is shared. The real differences are two keystrokes and
the timestamp log.

## Recommendation: move the shared actions to one "common" button

Why: the shared part is what keeps changing (LaneIOString handling, the CTSTracking
POST, the Dolphin5 reset, and the pending pool 2 Companion POST). One copy means
the two buttons can't drift apart.

### Proposed split

**Common button** (on a hidden or utility page, not the CTS page):
1. POST LaneIOString to CTSTracking
2. SpotifyPC Ctrl+F13
3. Dolphin5TCP `reset`
4. Wait 10 s
5. Reset `LaneIOString` to default

**Each of 3/2/7 and 3/3/7:**
1. Set local `Reset` = 1
2. Internal: press and release the common button
3. Its own GEN7 key (F14 or F13)
4. Its own Dolphin5Vicreo key (F14 or F15)
5. Next Heat only: timestamp exec
6. Wait 10 s, then reset local `Reset`

### Things to watch

1. **Keep the `Reset` flash on each unique button.** It uses `$(this:location)`.
   On the common button it would flash the common button instead. That would kill
   the flash on 3/2/7 and 3/3/7, and on the 3/2/6 mirror. So the 10 s wait
   appears in both places, which is fine.
2. **Order becomes "roughly at the same time".** Pressing another button doesn't
   wait for its actions to finish. The common actions and the unique actions run
   side by side, so the GEN7 key and the Dolphin5 TCP reset no longer fire in a
   fixed order. They go to different machines, so it's probably harmless. If two
   actions must happen in order (for example Dolphin5 TCP reset before the Vicreo
   F14/F15), keep both of them on the same button.
3. **Keep the LaneIOString POST and the LaneIOString clear together** on the common
   button, so the POST always reads the value before it's wiped.
4. **The common button shouldn't be pressable on its own** during a meet. Pressed
   alone, it would reset Dolphin and clear lanes without advancing GEN7.

## Open questions before implementing

- **Timestamp:** is logging only on Next Heat deliberate? If every advance should
  be logged, move the exec to the common button.
- **Key order:** what do the GEN7 F13/F14 and Dolphin5Vicreo F14/F15 keys do in
  the AHK scripts, and does the Vicreo key need to land before or after the
  Dolphin5 TCP `reset`? This decides whether the Dolphin5 TCP reset can move to
  the common button or has to stay next to the Vicreo key.
- **Default:** should the switcher (3/2/6) default to Next Heat or Next Event?
  Next Heat for now.
