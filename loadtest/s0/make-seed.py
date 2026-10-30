"""계좌가 많을 때(명령행 한도를 넘길 때) 시딩 계좌 목록을 설정 파일로 만든다.
사용: make-seed.py <출력 경로> <계좌 수> <계좌당 종목별 보유 수량> <종목코드...>
계좌 90001은 잔고만, 90002부터 계좌 수만큼은 잔고와 종목별 보유 수량을 가진다."""
import sys

path, accounts, holding = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
stocks = sys.argv[4:]
with open(path, "w", encoding="utf-8") as out:
    out.write("account-worker:\n  seed-accounts:\n")
    out.write("    - account-id: 90001\n      balance: 100000000000\n      margin-rate: 0.40\n")
    for k in range(1, accounts + 1):
        out.write("    - account-id: %d\n      balance: 100000000\n      margin-rate: 0.40\n      holdings:\n" % (90001 + k))
        for stock in stocks:
            out.write('        "%s": %d\n' % (stock, holding))
