"""계좌가 많을 때(명령행 한도를 넘길 때) 시딩 계좌 목록을 설정 파일로 만든다.
사용: make-seed.py <출력 경로> <계좌 수> <계좌당 종목별 보유 수량> <종목코드...>
계좌 90001은 잔고만, 90002부터 계좌 수만큼은 잔고와 종목별 보유 수량을 가진다.
환경변수 BUYER_ACCOUNTS=<수>와 BUYER_BALANCE=<잔고>를 주면 그 뒤 번호(90002 + 계좌 수부터)로 보유 없이 지정 잔고만 가진 매수 전용 계좌를 더한다(체결 섞기용).
둘 중 하나만 주면 거부한다.
환경변수 HOLDINGS_FILE=<경로>를 주면(「HOLD 계좌 종목 수량」 줄) 매도 계좌의 보유를 종목 인자 대신 그 파일의 값으로 만든다(계좌마다 종목이 다른 묶음 모드용)."""
import os
import sys

path, accounts, holding = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
stocks = sys.argv[4:]
buyer_accounts = os.environ.get("BUYER_ACCOUNTS")
buyer_balance = os.environ.get("BUYER_BALANCE")
if (buyer_accounts is None) != (buyer_balance is None):
    sys.exit("BUYER_ACCOUNTS와 BUYER_BALANCE는 함께 줘야 한다")
buyers = int(buyer_accounts) if buyer_accounts is not None else 0
holdings_file = os.environ.get("HOLDINGS_FILE")
file_holdings = {}
if holdings_file:
    for line in open(holdings_file, encoding="utf-8"):
        parts = line.split()
        if len(parts) == 4 and parts[0] == "HOLD":
            file_holdings.setdefault(int(parts[1]), []).append((parts[2], int(parts[3])))
with open(path, "w", encoding="utf-8") as out:
    out.write("account-worker:\n  seed-accounts:\n")
    out.write("    - account-id: 90001\n      balance: 100000000000\n      margin-rate: 0.40\n")
    for k in range(1, accounts + 1):
        out.write("    - account-id: %d\n      balance: 100000000\n      margin-rate: 0.40\n      holdings:\n" % (90001 + k))
        if holdings_file:
            for stock, quantity in file_holdings.get(90001 + k, []):
                out.write('        "%s": %d\n' % (stock, quantity))
        else:
            for stock in stocks:
                out.write('        "%s": %d\n' % (stock, holding))
    for k in range(1, buyers + 1):
        out.write("    - account-id: %d\n      balance: %d\n      margin-rate: 0.40\n" % (90001 + accounts + k, int(buyer_balance)))
