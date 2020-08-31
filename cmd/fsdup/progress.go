package main

import (
	"context"
	"fmt"
	"strings"
	"time"
	"github.com/datto/copyondemand"
)

const (
	progressEnds                = "|"
	progressPending             = "-"
	progressComplete            = "="
	progressBarWidth            = 30
	byteSpeedWindowMilliseconds = 5000
)

var previousOutputWidth int

func outputStatus(ctx context.Context, fileBackedDevice *copyondemand.FileBackedDevice) {
	previousOutputWidth = -1
	for true {
		select {
		case <-ctx.Done():
			return
		case <-time.After(1 * time.Second):
			outputProgressBar(fileBackedDevice)
		}
	}
}

func outputProgressBar(fileBackedDevice *copyondemand.FileBackedDevice) {
	totalBlocks := (fileBackedDevice.Source.Size / copyondemand.BlockSize) + 1
	blockProgress := fileBackedDevice.TotalSyncedBlocks()
	percentComplete := (blockProgress * 100) / totalBlocks
	backingReadRate := fileBackedDevice.SampleRate(copyondemand.BackingRead, byteSpeedWindowMilliseconds)
	backingWriteRate := fileBackedDevice.SampleRate(copyondemand.BackingWrite, byteSpeedWindowMilliseconds)
	sourceReadRate := fileBackedDevice.SampleRate(copyondemand.SourceRead, byteSpeedWindowMilliseconds)
	maxBandwidth := fileBackedDevice.GetBackgroundCopyRate()
	barsComplete := (percentComplete * progressBarWidth) / 100
	barsNotComplete := progressBarWidth - barsComplete

	fmt.Print("\r")
	if previousOutputWidth != -1 {
		fmt.Printf("%s\r", strings.Repeat(" ", previousOutputWidth))
	}

	progressBar := fmt.Sprintf(
		"%s%s%s%s %d%% br %s/s bw %s/s sr %s mb %s/s",
		progressEnds,
		strings.Repeat(progressComplete, int(barsComplete)),
		strings.Repeat(progressPending, int(barsNotComplete)),
		progressEnds,
		percentComplete,
		copyondemand.BytesToHumanReadable(backingReadRate),
		copyondemand.BytesToHumanReadable(backingWriteRate),
		copyondemand.BytesToHumanReadable(sourceReadRate),
		copyondemand.BytesToHumanReadable(maxBandwidth),
	)
	previousOutputWidth = len(progressBar)
	fmt.Print(progressBar)
}
