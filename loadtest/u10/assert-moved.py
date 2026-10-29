#!/usr/bin/env python3
"""종목 slot 하나가 이전 담당 주소에서 새 담당 주소로 실제로 바뀌었는지 맵 기록으로 단언한다.

사용: assert-moved.py <맵 기록 파일(slot<TAB>시각<TAB>값[<TAB>timestampType])> <slot> <이전 주소> <새 주소>
통과(종료 코드 0): 그 slot의 기록에 이전 주소(평상시 값 또는 「;moving」 값)가 먼저 있고, 마지막 기록이 새 주소의 평상시 값이다.
실패(종료 코드 1): 그 밖. 이동 완료를 맵 개수(32/32)로만 판정하면 협력 배정이 반대쪽 slot을 옮겨도 통과하므로 이 단언을 따로 둔다.
"""
import sys


def main():
    path, slot, old, new = sys.argv[1], int(sys.argv[2]), sys.argv[3], sys.argv[4]
    values = []
    for line in open(path):
        parts = line.rstrip('\n').split('\t')
        if len(parts) >= 3 and parts[0].isdigit() and int(parts[0]) == slot:
            values.append(parts[2])
    if not values:
        print('실패: slot %d의 맵 기록이 없다' % slot)
        return 1
    had_old = any(v == old or v == old + ';moving' for v in values)
    final = values[-1]
    if had_old and final == new:
        print('통과: slot %d가 %s에서 %s로 바뀌었다(기록 %d건)' % (slot, old, new, len(values)))
        return 0
    print('실패: slot %d가 %s에서 %s로 바뀌지 않았다(이전 주소 기록 있음=%s, 마지막 값=%s)' % (slot, old, new, had_old, final))
    return 1


sys.exit(main())
