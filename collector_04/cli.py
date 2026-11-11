import argparse
import json
import sys
from pathlib import Path
from .store import RecordStore
from .capture import capture
from .scheduler import run_scenario, status_projection


def main(argv=None):
    parser = argparse.ArgumentParser(description='表单录入队列的断点与人工接管')
    parser.add_argument("--database", default="collector.sqlite3")
    parser.add_argument("--init", action="store_true")
    parser.add_argument("--capture", metavar="FIXTURE_ROOT")
    parser.add_argument("--route", default='/tasks')
    parser.add_argument("--observed-at", default="2026-11-11T10:00:00Z")
    parser.add_argument("--run", metavar="SCENARIO", help="执行场景文件描述的录入工作流")
    parser.add_argument("--output", metavar="OUTPUT", help="业务结果投影输出文件")
    parser.add_argument("--status", action="store_true", help="只查询状态库中的最终处理清单，不执行")
    parser.add_argument("--portal-database", help="覆盖场景中的 portal 替身库路径")
    parser.add_argument("--pause-before-submit", action=argparse.BooleanOptionalAction, default=None,
                        help="覆盖场景选项：准备后暂停，不提交")
    parser.add_argument("--resume", action=argparse.BooleanOptionalAction, default=None,
                        help="覆盖场景选项：主流程后新建执行对象恢复未确认的提交")
    args = parser.parse_args(argv)
    if args.capture:
        print(json.dumps(capture(args.capture, args.route, args.database, args.observed_at), ensure_ascii=False))
        return 0
    if args.init:
        store = RecordStore(args.database)
        store.initialize()
        print(json.dumps({"stored": store.count()}))
        return 0
    if args.run:
        try:
            result = run_scenario(args.run, args.database,
                                  pause_override=args.pause_before_submit,
                                  resume_override=args.resume,
                                  portal_override=args.portal_database)
        except (ValueError, OSError) as error:
            print(f"run failed: {error}", file=sys.stderr)
            return 2
        text = json.dumps(result, ensure_ascii=False, indent=2)
        if args.output:
            Path(args.output).write_text(text + "\n")
        print(text)
        return 0
    if args.status:
        try:
            result = status_projection(args.database, portal_override=args.portal_database)
        except (ValueError, OSError) as error:
            print(f"status failed: {error}", file=sys.stderr)
            return 2
        text = json.dumps(result, ensure_ascii=False, indent=2)
        if args.output:
            Path(args.output).write_text(text + "\n")
        print(text)
        return 0
    parser.print_help()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
