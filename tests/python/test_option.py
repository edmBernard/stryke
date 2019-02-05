
import python.stryke as sy

def test_options_method():
    options = sy.WriterOptions()

    options.disable_lock_file(True)
    options.disable_lock_file()
    options.enable_suffix_timestamp(True)
    options.enable_suffix_timestamp()
    options.set_batch_size(1)
    options.set_batch_max(1)
    options.set_stripe_size(1)
    options.set_cron(1)
