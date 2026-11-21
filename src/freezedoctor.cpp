#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <memory>
#include <new>
#include <sched.h>
#include <signal.h>
#include <sstream>
#include <string>
#include <sys/mman.h>
#include <sys/prctl.h>
#include <sys/resource.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#include <vector>
using namespace std;
constexpr int MAX_WORKERS = 16;
constexpr unsigned CAPACITY = 131072;
constexpr int64_t SAMPLE_NS = 500000000LL;
constexpr int64_t PERIOD_NS = 10000000LL;
constexpr int64_t SERVICE_NS = 2000000LL;

struct Measurement {
    double response_ms{}, wake_ms{};
};
struct Shared {
    std::atomic<unsigned> count{0};
    std::array<Measurement, CAPACITY> samples{};
};
static_assert(std::atomic<unsigned>::is_always_lock_free,
              "The shared probe counter must be lock-free on this Linux target");
struct Stats {
    unsigned n{};
    double mean{}, p95{}, maximum{},minimum{};
};
struct Proc {
    unsigned long long ticks{}, wait_ns{};
    char state{};
    int nice{};
    bool valid{};
};
static volatile sig_atomic_t interrupted;
static std::array<pid_t, MAX_WORKERS + 1> children{};
static unsigned child_count;
static Shared *shared;
static FILE *csv;
static void on_signal(int sig) {
    (void)sig;
    interrupted = 1;
}
static std::int64_t now_ns(clockid_t clock) {
    struct timespec t;
    if (clock_gettime(clock, &t)) {
        perror("clock_gettime");
        exit(1);
    }
    return static_cast<std::int64_t>(t.tv_sec) * 1000000000LL + t.tv_nsec;
}
static void sleep_until(std::int64_t when) {
    struct timespec t = {when / 1000000000LL, when % 1000000000LL};
    int rc;
    while ((rc = clock_nanosleep(CLOCK_MONOTONIC, TIMER_ABSTIME, &t, nullptr)) == EINTR &&
           !interrupted) {
    }
    if (rc && rc != EINTR) {
        errno = rc;
        perror("clock_nanosleep");
        exit(1);
    }
}
static void cleanup() {
    for (unsigned i = 0; i < child_count; i++)
        if (children[i] > 0)
            kill(children[i], SIGTERM);
    for (unsigned i = 0; i < child_count; i++)
        while (waitpid(children[i], nullptr, 0) < 0 && errno == EINTR) {
        }
    child_count = 0;
}
static void pin(int cpu) {
    cpu_set_t mask;
    CPU_ZERO(&mask);
    CPU_SET(cpu, &mask);
    if (sched_setaffinity(0, sizeof(mask), &mask)) {
        perror("sched_setaffinity");
        _exit(1);
    }
}
static pid_t spawn(int cpu, bool probe) {
    pid_t parent = getpid();
    pid_t pid = fork();

    if (pid < 0) {
        perror("fork");
        exit(1);
    }

    if (pid > 0) {
        children[child_count++] = pid;
        return pid;
    }

    signal(SIGINT, SIG_DFL);
    signal(SIGTERM, SIG_DFL);

    if (prctl(PR_SET_PDEATHSIG, SIGTERM) < 0)
        _exit(1);

    if (getppid() != parent)
        _exit(1);

    pin(cpu);

    prctl(
        PR_SET_NAME,
        probe ? "fd-probe" : "fd-cpu-worker",
        0, 0, 0
    );

    volatile uint64_t value = 1;

    if (!probe) {
        while (true) {
            value = value * 6364136223846793005ULL + 1;
        }
    }

    int64_t release = now_ns(CLOCK_MONOTONIC) + PERIOD_NS;

    while (true) {
        sleep_until(release);

        int64_t wake = now_ns(CLOCK_MONOTONIC);
        int64_t cpu_start = now_ns(CLOCK_PROCESS_CPUTIME_ID);

        do {
            for (int j = 0; j < 1000; j++) {
                value = value * 6364136223846793005ULL + 1;
            }
        } while (
            now_ns(CLOCK_PROCESS_CPUTIME_ID) - cpu_start < SERVICE_NS
        );

        int64_t finish = now_ns(CLOCK_MONOTONIC);

        unsigned n = shared->count.load(memory_order_relaxed);

        if (n < CAPACITY) {
            shared->samples[n] = Measurement{
                (finish - release) / 1e6,
                (wake - release) / 1e6
            };

            shared->count.store(n + 1, memory_order_release);
        }

        release += PERIOD_NS;

        if (release < finish) {
            release = finish + PERIOD_NS;
        }
    }
}
// /proc/PID/stat fields after the final ')' begin at field 3 (state).
static Proc observe(pid_t pid) {
    Proc p{};
    const std::string directory = "/proc/" + std::to_string(pid);
    std::ifstream stat_file(directory + "/stat");
    std::string line;
    if (!std::getline(stat_file, line))
        return p;
    const auto close = line.rfind(')');
    if (close == std::string::npos)
        return p;
    std::istringstream fields(line.substr(close + 1));
    std::string ignored;
    unsigned long long user_ticks{}, system_ticks{};
    if (!(fields >> p.state))
        return p;
    for (int field = 4; field <= 13; ++field) {
        if (!(fields >> ignored))
            return p;
    }
    if (!(fields >> user_ticks >> system_ticks))
        return p;
    for (int field = 16; field <= 18; ++field) {
        if (!(fields >> ignored))
            return p;
    }
    if (!(fields >> p.nice))
        return p;
    p.ticks = user_ticks + system_ticks;
    p.valid = true;
    std::ifstream schedstat(directory + "/schedstat");
    unsigned long long runtime{};
    if (!(schedstat >> runtime >> p.wait_ns))
        p.wait_ns = 0;
    return p;
}

static Stats stats(unsigned begin, unsigned end) {
    Stats s{};

    if (end <= begin)
        return s;

    s.n = end - begin;
    std::vector<double> v(s.n);

    for (unsigned i = 0; i < s.n; i++) {
        v[i] = shared->samples[begin + i].response_ms;
        s.mean += v[i];
    }

    s.mean /= s.n;

    std::sort(v.begin(), v.end());

    s.minimum = v[0];
    s.maximum = v[s.n - 1];
    s.p95 = v[static_cast<unsigned>(std::ceil(.95 * s.n)) - 1];

    return s;
}

struct Phase {
    unsigned begin{}, end{};
    Stats stats{};
};
static int streak, max_streak;
static bool detected;
static pid_t contributor;
static double contributor_cpu;
static Phase phase(const char *name, double seconds, pid_t *workers, int count, double threshold,
                   bool analyze) {
    Phase result{};
    result.begin = shared->count.load(std::memory_order_acquire);
    unsigned previous = result.begin;
    std::array<Proc, MAX_WORKERS> prior{};
    for (int i = 0; i < count; i++)
        prior[i] = observe(workers[i]);
    std::int64_t start = now_ns(CLOCK_MONOTONIC), last = start,
                 end = start + static_cast<std::int64_t>(seconds * 1e9), next = start;
    printf("\n[%s] %.1f seconds\n", name, seconds);
    while (!interrupted && next < end) {
        next += SAMPLE_NS;
        if (next > end)
            next = end;
        sleep_until(next);
        if (interrupted)
            break;
        std::int64_t current = now_ns(CLOCK_MONOTONIC);
        unsigned n = shared->count.load(std::memory_order_acquire);
        Stats window = stats(previous, n);
        double total = 0, best = 0;
        pid_t best_pid = 0;
        for (int i = 0; i < count; i++) {
            Proc p = observe(workers[i]);
            if (!p.valid || p.state == 'Z') {
                fprintf(stderr, "Worker %d exited unexpectedly\n", workers[i]);
                exit(1);
            }
            double cpu = (p.ticks - prior[i].ticks) * 100.0 / sysconf(_SC_CLK_TCK) /
                         ((current - last) / 1e9);
            total += cpu;
            if (cpu > best) {
                best = cpu;
                best_pid = workers[i];
            }
            prior[i] = p;
        }
        bool abnormal = window.n && window.mean > threshold && total >= 30;
        if (analyze) {
            streak = abnormal ? streak + 1 : 0;
            if (streak > max_streak)
                max_streak = streak;
            if (streak >= 3) {
                detected = true;
                contributor = best_pid;
                contributor_cpu = best;
            }
        }
        printf("  samples=%-3u response=%7.3f ms  p95=%7.3f ms  workload CPU=%5.1f%% %s\n",
               window.n, window.mean, window.p95, total,
               analyze && abnormal ? "DELAY + CONTENTION" : "");
        for (unsigned i = previous; i < n; i++)
            fprintf(csv, "%s,%u,%.6f,%.6f,%.3f\n", name, i, shared->samples[i].response_ms,
                    shared->samples[i].wake_ms, total);
        previous = n;
        last = current;
    }
    result.end = previous;
    result.stats = stats(result.begin, result.end);
    return result;
}

static void json_stats(FILE *f, const char *key, Stats s, bool comma) {
    fprintf(
        f,
        "  \"%s\": {"
        "\"samples\": %u, "
        "\"mean_ms\": %.6f, "
        "\"min_ms\": %.6f, "
        "\"p95_ms\": %.6f, "
        "\"max_ms\": %.6f"
        "}%s\n",
        key,
        s.n,
        s.mean,
        s.minimum,
        s.p95,
        s.maximum,
        comma ? "," : ""
    );
}

static void usage(const char *exe) {
    printf("Usage: %s [--seconds 2..60] [--workers 1..16] [--cpu N] [--report FILE] [--csv FILE] "
           "[--no-recovery]\nDefault: 4 seconds per phase, 4 workers, first allowed CPU, "
           "report.json, samples.csv.\nNo root required. Only child workloads are reniced.\n",
           exe);
}
int main(int argc, char **argv) {
    double seconds = 4;
    int count = 4;
    int cpu = -1;
    bool recover = true;

    const char *report = "report.json";
    const char *csv_path = "samples.csv";

    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--help")) {
            usage(argv[0]);
            return 0;
        }

        if (!strcmp(argv[i], "--no-recovery")) {
            recover = false;
            continue;
        }

        if (i + 1 >= argc) {
            usage(argv[0]);
            return 2;
        }

        const char *key = argv[i];
        const char *value = argv[++i];
        char *tail = nullptr;

        if (!strcmp(key, "--report")) {
            report = value;
        } else if (!strcmp(key, "--csv")) {
            csv_path = value;
        } else if (!strcmp(key, "--seconds")) {
            errno = 0;
            seconds = strtod(value, &tail);

            if (tail == value || *tail || errno == ERANGE ||
                !isfinite(seconds) || seconds < 2 || seconds > 60) {
                fprintf(stderr, "Seconds must be between 2 and 60.\n");
                return 2;
            }
        } else if (!strcmp(key, "--workers")) {
            errno = 0;
            long number = strtol(value, &tail, 10);

            if (tail == value || *tail || errno == ERANGE ||
                number < 1 || number > MAX_WORKERS) {
                fprintf(
                    stderr,
                    "Workers must be between 1 and %d.\n",
                    MAX_WORKERS
                );
                return 2;
            }

            count = static_cast<int>(number);
        } else if (!strcmp(key, "--cpu")) {
            errno = 0;
            long number = strtol(value, &tail, 10);

            if (tail == value || *tail || errno == ERANGE ||
                number < 0 || number >= CPU_SETSIZE) {
                fprintf(stderr, "Invalid CPU number.\n");
                return 2;
            }

            cpu = static_cast<int>(number);
        } else {
            usage(argv[0]);
            return 2;
        }
    }

    if (!strcmp(report, csv_path)) {
        fprintf(stderr, "Report and CSV paths must differ.\n");
        return 2;
    }

    cpu_set_t allowed;

    if (sched_getaffinity(0, sizeof(allowed), &allowed)) {
        perror("sched_getaffinity");
        return 1;
    }

    if (cpu < 0) {
        for (int i = 0; i < CPU_SETSIZE; i++) {
            if (CPU_ISSET(i, &allowed)) {
                cpu = i;
                break;
            }
        }
    }

    if (cpu < 0 || !CPU_ISSET(cpu, &allowed)) {
        fprintf(stderr, "CPU is outside permitted affinity.\n");
        return 2;
    }

    csv = fopen(csv_path, "w");

    if (!csv) {
        perror(csv_path);
        return 1;
    }

    FILE *out = fopen(report, "w");

    if (!out) {
        perror(report);
        fclose(csv);
        return 1;
    }

    void *mapping = mmap(
        nullptr,
        sizeof(Shared),
        PROT_READ | PROT_WRITE,
        MAP_SHARED | MAP_ANONYMOUS,
        -1,
        0
    );

    if (mapping == MAP_FAILED) {
        perror("mmap");
        fclose(csv);
        fclose(out);
        return 1;
    }

    const auto release_shared = [](Shared *buffer) {
        cleanup();
        buffer->~Shared();
        munmap(buffer, sizeof(Shared));
    };

    unique_ptr<Shared, decltype(release_shared)> shared_owner(
        new (mapping) Shared{},
        release_shared
    );

    shared = shared_owner.get();

    atexit(cleanup);

    struct sigaction sa{};
    sa.sa_handler = on_signal;
    sigemptyset(&sa.sa_mask);

    sigaction(SIGINT, &sa, nullptr);
    sigaction(SIGTERM, &sa, nullptr);

    setvbuf(stdout, nullptr, _IOLBF, 0);

    fprintf(
        csv,
        "phase,sample,response_ms,wake_delay_ms,workload_cpu_percent\n"
    );

    printf(
        "FreezeDoctor | real Linux scheduling experiment\n"
        "CPU %d | %d workers | probe: 2 ms CPU service every 10 ms\n",
        cpu,
        count
    );

    pid_t probe = spawn(cpu, true);
    array<pid_t, MAX_WORKERS> workers{};

    Phase baseline = phase(
        "baseline", seconds, nullptr, 0, 0, false
    );

    double threshold = max(
        baseline.stats.mean * 1.5,
        baseline.stats.mean + 1.0
    );

    if (interrupted) {
        fclose(csv);
        fclose(out);
        return 130;
    }

    for (int i = 0; i < count; i++) {
        workers[i] = spawn(cpu, false);
    }

    Phase before = phase(
        "contention",
        seconds,
        workers.data(),
        count,
        threshold,
        true
    );

    bool applied = false;
    int adjusted = 0;
    int64_t intervention_ns = 0;

    array<int, MAX_WORKERS> original_nice{};
    array<Proc, MAX_WORKERS> worker_before{};
    array<Proc, MAX_WORKERS> worker_after{};

    for (int i = 0; i < count; i++) {
        worker_before[i] = observe(workers[i]);
    }

    if (!interrupted && detected && recover) {
        intervention_ns = now_ns(CLOCK_MONOTONIC);

        printf(
            "\nDETECTED: %d consecutive abnormal windows; "
            "contributor PID %d, %.1f%% CPU\n",
            max_streak,
            contributor,
            contributor_cpu
        );

        for (int i = 0; i < count; i++) {
            errno = 0;
            original_nice[i] = getpriority(
                PRIO_PROCESS, workers[i]
            );

            if (errno) {
                perror("getpriority");
                continue;
            }

            if (setpriority(PRIO_PROCESS, workers[i], 19)) {
                perror("setpriority");
                continue;
            }

            errno = 0;
            int actual = getpriority(PRIO_PROCESS, workers[i]);

            if (!errno && actual == 19 && original_nice[i] < 19) {
                adjusted++;

                printf(
                    "Recovery: PID %d nice %d -> %d\n",
                    workers[i],
                    original_nice[i],
                    actual
                );
            }
        }

        applied = adjusted == count;
    }

    Phase after{};

    if (!interrupted) {
        after = phase(
            "verification",
            seconds,
            workers.data(),
            count,
            threshold,
            false
        );
    }

    bool verified =
        !interrupted &&
        baseline.stats.n >= 10 &&
        detected &&
        applied &&
        before.stats.n >= 10 &&
        after.stats.n >= 10 &&
        after.stats.mean < before.stats.mean * 0.9 &&
        after.stats.p95 < before.stats.p95;

    double improvement = 0;

    if (before.stats.mean > 0) {
        improvement =
            (before.stats.mean - after.stats.mean) * 100.0 /
            before.stats.mean;
    }

    Proc probe_info = observe(probe);

    for (int i = 0; i < count; i++) {
        worker_after[i] = observe(workers[i]);
    }

    fprintf(
        out,
        "{\n"
        "  \"schema_version\": 1,\n"
        "  \"cpu\": %d,\n"
        "  \"workers\": %d,\n"
        "  \"probe_pid\": %d,\n"
        "  \"contributor_pid\": %d,\n"
        "  \"contributor_cpu_percent\": %.3f,\n"
        "  \"threshold_ms\": %.6f,\n"
        "  \"persistent_windows\": %d,\n"
        "  \"contention_detected\": %s,\n"
        "  \"priority_adjustment_applied\": %s,\n"
        "  \"adjusted_workers\": %d,\n"
        "  \"recovery_verified\": %s,\n"
        "  \"interrupted\": %s,\n"
        "  \"improvement_percent\": %.3f,\n"
        "  \"probe_scheduler_wait_ns\": %llu,\n",
        cpu,
        count,
        probe,
        contributor,
        contributor_cpu,
        threshold,
        max_streak,
        detected ? "true" : "false",
        applied ? "true" : "false",
        adjusted,
        verified ? "true" : "false",
        interrupted ? "true" : "false",
        improvement,
        probe_info.wait_ns
    );

    fprintf(
        out,
        "  \"intervention_monotonic_ns\": %lld,\n"
        "  \"workload_evidence\": [\n",
        static_cast<long long>(intervention_ns)
    );

    for (int i = 0; i < count; i++) {
        fprintf(
            out,
            "    {\"pid\": %d, \"state_before\": \"%c\", "
            "\"nice_before\": %d, \"nice_after\": %d, "
            "\"cpu_ticks_before\": %llu, "
            "\"cpu_ticks_after\": %llu}%s\n",
            workers[i],
            worker_before[i].state ? worker_before[i].state : '?',
            worker_before[i].nice,
            worker_after[i].nice,
            worker_before[i].ticks,
            worker_after[i].ticks,
            i + 1 < count ? "," : ""
        );
    }

    fprintf(out, "  ],\n");

    json_stats(out, "baseline", baseline.stats, true);
    json_stats(out, "before", before.stats, true);
    json_stats(out, "after", after.stats, false);

    fprintf(out, "}\n");

    bool io_error = ferror(csv) || ferror(out);

    if (fclose(csv))
        io_error = true;

    if (fclose(out))
        io_error = true;

    cleanup();

    const char *result;

    if (interrupted)
        result = "INTERRUPTED";
    else if (verified)
        result = "RECOVERY VERIFIED";
    else
        result = "RECOVERY NOT VERIFIED";

    printf(
        "\n%s\n"
        "Before %.3f ms -> after %.3f ms "
        "(%.1f%% mean improvement)\n"
        "Reports: %s, %s\n",
        result,
        before.stats.mean,
        after.stats.mean,
        improvement,
        report,
        csv_path
    );

    if (io_error) {
        fprintf(stderr, "Failed writing report.\n");
        return 1;
    }

    return interrupted ? 130 : 0;
}
